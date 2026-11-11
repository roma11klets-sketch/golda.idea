// Заменяет среду Claude: комнаты через WebSocket этого сервера и ИИ через /api/ai.
(function () {
  var H = (window.GAME_SERVER || '').replace(/\/$/, '');
  var WS = H ? H.replace(/^http/, 'ws') + '/ws' : (location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/ws';
  function join(name) {
    return new Promise(function (res, rej) {
      var ws, me = null, peers = [], cbs = [], pres = {}, closed = false, done = false;
      function snap() { return peers.map(function (p) { return { peer: p.peer, presence: p.presence, isMe: p.peer === me }; }); }
      var room = {
        onPeers: function (f) { cbs.push(f); if (peers.length) f({ peers: snap() }); return function () { cbs = cbs.filter(function (x) { return x !== f; }); }; },
        presence: function (p) { Object.assign(pres, p); if (ws && ws.readyState === 1) ws.send(JSON.stringify({ t: 'pres', patch: p })); },
        emit: function () {}, on: function () { return function () {}; }, peers: snap,
        leave: function () { closed = true; try { ws.close(); } catch (e) {} }
      };
      function open() {
        ws = new WebSocket(WS);
        ws.onopen = function () {
          ws.send(JSON.stringify({ t: 'join', room: name }));
          if (Object.keys(pres).length) ws.send(JSON.stringify({ t: 'pres', patch: pres }));
        };
        ws.onmessage = function (e) {
          var m; try { m = JSON.parse(e.data); } catch (x) { return; }
          if (m.t === 'hello') { me = m.id; if (!done) { done = true; res(room); } }
          else if (m.t === 'peers') { peers = m.peers; var s = snap(); cbs.forEach(function (f) { f({ peers: s }); }); }
          else if (m.t === 'full' && !done) { done = true; rej({ code: 'full' }); }
        };
        ws.onclose = function () { if (!closed) setTimeout(open, 2000); };
        ws.onerror = function () {};
      }
      open();
      setTimeout(function () { if (!done) { done = true; rej({ code: 'timeout' }); } }, 8000);
    });
  }
  var sample = {
    json: async function (prompt) {
      var r = await fetch(H + '/api/ai', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ prompt: prompt }) });
      var j = {}; try { j = await r.json(); } catch (e) {}
      if (!r.ok) throw { code: j.error || 'http_' + r.status };
      var t = String(j.text || ''), a = t.indexOf('{'), b = t.lastIndexOf('}');
      if (a < 0 || b < a) throw { code: 'bad_json' };
      return JSON.parse(t.slice(a, b + 1));
    }
  };
  window.claude = { use: async function (n) { return n === 'room' ? { join: join } : n === 'sample' ? sample : null; } };
})();
