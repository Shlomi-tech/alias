// שיחה קולית בתוך המשחק: חיבור ישיר בין הטלפונים (WebRTC),
// ו-Firebase משמש רק כדי שהטלפונים ימצאו אחד את השני.
// כל טלפון מתחבר לכל האחרים, ולכן זה מתאים לעד כ-8 משתתפים.

const ICE_SERVERS = [{ urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302'] }];
const rid = () => Math.random().toString(36).slice(2, 10);

export function createVoice({ db, ref, onValue, set, remove, onDisconnect, pid, onChange }) {
  let code = null, sid = null, stream = null, muted = false, joined = false, joining = false;
  let members = {};
  let membersUnsub = null;
  const peers = new Map(); // pid -> { pc, sid, key, unsubs, state, cands, seen, audio, busy }
  const meters = new Map(); // pid -> analyser
  let actx = null, meterTimer = null;
  const path = (p) => `rooms/${code}/${p}`;
  const audioBox = () => document.getElementById('voice-audio');

  async function join(roomCode) {
    if (joined || joining) return;
    joining = true;
    try {
      stream = await navigator.mediaDevices.getUserMedia({
        audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: true }, video: false,
      });
    } finally { joining = false; }
    code = roomCode; sid = rid(); muted = false; joined = true;
    const me = ref(db, path(`voice/${pid}`));
    onDisconnect(me).remove();
    await set(me, { sid, muted: false, at: Date.now() });
    try { actx = actx || new (window.AudioContext || window.webkitAudioContext)(); actx.resume(); } catch {}
    addMeter(pid, stream);
    membersUnsub = onValue(ref(db, path('voice')), (s) => { members = s.val() || {}; sync(); onChange(); });
    meterTimer = setInterval(meter, 150);
    onChange();
  }

  function leave() {
    if (!joined) return;
    joined = false;
    membersUnsub && membersUnsub(); membersUnsub = null;
    [...peers.keys()].forEach(closePeer);
    remove(ref(db, path(`voice/${pid}`))).catch(() => {});
    stream && stream.getTracks().forEach((t) => t.stop()); stream = null;
    clearInterval(meterTimer); meters.clear(); members = {};
    onChange();
  }

  function toggleMute() {
    if (!joined) return;
    muted = !muted;
    stream.getAudioTracks().forEach((t) => { t.enabled = !muted; });
    set(ref(db, path(`voice/${pid}/muted`)), muted).catch(() => {});
    onChange();
  }

  // מתחבר לכל מי שנכנס לשיחה, ומתנתק ממי שיצא
  function sync() {
    for (const [p, peer] of peers) { const m = members[p]; if (!m || m.sid !== peer.sid) closePeer(p); }
    for (const [p, m] of Object.entries(members)) { if (p !== pid && !peers.has(p) && m?.sid) connect(p, m.sid); }
  }

  function connect(p, psid) {
    const offerer = pid < p; // צד אחד קבוע יוזם את החיבור
    const key = offerer ? `${pid}_${sid}__${p}_${psid}` : `${p}_${psid}__${pid}_${sid}`;
    const base = path(`rtc/${key}`);
    const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });
    const peer = { pc, sid: psid, key, unsubs: [], state: 'connecting', cands: {}, seen: new Set(), audio: null, busy: false };
    peers.set(p, peer);
    stream.getTracks().forEach((t) => pc.addTrack(t, stream));
    onDisconnect(ref(db, base)).remove();

    pc.ontrack = (e) => {
      if (!peer.audio) {
        peer.audio = document.createElement('audio');
        peer.audio.autoplay = true; peer.audio.playsInline = true;
        audioBox()?.appendChild(peer.audio);
      }
      peer.audio.srcObject = e.streams[0];
      peer.audio.play().catch(() => {});
      addMeter(p, e.streams[0]);
    };
    pc.onicecandidate = (e) => { if (e.candidate) set(ref(db, `${base}/c_${pid}/${rid()}`), e.candidate.toJSON()).catch(() => {}); };
    pc.onconnectionstatechange = () => { peer.state = pc.connectionState; onChange(); };

    const applyCands = () => {
      if (!pc.remoteDescription) return;
      for (const [k, c] of Object.entries(peer.cands)) {
        if (peer.seen.has(k)) continue;
        peer.seen.add(k); pc.addIceCandidate(c).catch(() => {});
      }
    };
    peer.unsubs.push(onValue(ref(db, `${base}/c_${p}`), (s) => { peer.cands = s.val() || {}; applyCands(); }));

    if (offerer) {
      (async () => {
        const o = await pc.createOffer();
        await pc.setLocalDescription(o);
        await set(ref(db, `${base}/offer`), { type: o.type, sdp: o.sdp });
      })().catch(() => { peer.state = 'failed'; onChange(); });
      peer.unsubs.push(onValue(ref(db, `${base}/answer`), async (s) => {
        const a = s.val();
        if (!a || pc.remoteDescription || peer.busy) return;
        peer.busy = true;
        try { await pc.setRemoteDescription(a); applyCands(); } catch {} finally { peer.busy = false; }
      }));
    } else {
      peer.unsubs.push(onValue(ref(db, `${base}/offer`), async (s) => {
        const o = s.val();
        if (!o || pc.remoteDescription || peer.busy) return;
        peer.busy = true;
        try {
          await pc.setRemoteDescription(o);
          const a = await pc.createAnswer();
          await pc.setLocalDescription(a);
          await set(ref(db, `${base}/answer`), { type: a.type, sdp: a.sdp });
          applyCands();
        } catch { peer.state = 'failed'; onChange(); } finally { peer.busy = false; }
      }));
    }
  }

  function closePeer(p) {
    const peer = peers.get(p); if (!peer) return;
    peer.unsubs.forEach((u) => u());
    try { peer.pc.close(); } catch {}
    peer.audio?.remove();
    meters.delete(p);
    remove(ref(db, path(`rtc/${peer.key}`))).catch(() => {});
    peers.delete(p);
  }

  // זיהוי מי מדבר כרגע, כדי להאיר את השם שלו
  function addMeter(p, s) {
    try {
      if (!actx) return;
      const src = actx.createMediaStreamSource(s);
      const an = actx.createAnalyser(); an.fftSize = 512;
      src.connect(an);
      meters.set(p, an);
    } catch {}
  }
  const buf = new Uint8Array(512);
  function meter() {
    for (const [p, an] of meters) {
      an.getByteTimeDomainData(buf);
      let sum = 0; for (let i = 0; i < buf.length; i++) { const v = (buf[i] - 128) / 128; sum += v * v; }
      const talking = Math.sqrt(sum / buf.length) > 0.04 && !(p === pid ? muted : members[p]?.muted);
      document.querySelectorAll(`[data-vp="${p}"]`).forEach((el) => el.classList.toggle('talk', talking));
    }
  }

  return {
    join, leave, toggleMute,
    state: () => ({
      joined, joining, muted, members,
      peerState: (p) => (p === pid ? 'connected' : peers.get(p)?.state || 'connecting'),
    }),
  };
}
