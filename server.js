const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok');
    return;
  }
  res.writeHead(404);
  res.end();
});

const wss = new WebSocketServer({ server });

// rooms: Map<roomCode, Set<ws>>  — pure in-memory, never touches disk
const rooms = new Map();

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const room = (url.searchParams.get('room') || '').trim();

  if (!room) {
    ws.close(1008, 'room required');
    return;
  }

  if (!rooms.has(room)) rooms.set(room, new Set());
  const peers = rooms.get(room);

  if (peers.size >= 2) {
    ws.send(JSON.stringify({ type: 'room-full' }));
    ws.close(1008, 'room full');
    return;
  }

  const role = peers.size === 0 ? 'host' : 'guest';
  peers.add(ws);
  ws.send(JSON.stringify({ type: 'role', role }));

  if (peers.size === 2) {
    for (const peer of peers) {
      if (peer.readyState === WebSocket.OPEN) {
        peer.send(JSON.stringify({ type: 'ready' }));
      }
    }
  }

  ws.on('message', (data) => {
    for (const peer of peers) {
      if (peer !== ws && peer.readyState === WebSocket.OPEN) {
        peer.send(data.toString());
      }
    }
  });

  ws.on('close', () => {
    peers.delete(ws);
    for (const peer of peers) {
      if (peer.readyState === WebSocket.OPEN) {
        peer.send(JSON.stringify({ type: 'peer-left' }));
      }
    }
    if (peers.size === 0) rooms.delete(room);
  });
});

server.listen(PORT, () => console.log(`Relay de sinalização ouvindo na porta ${PORT}`));
