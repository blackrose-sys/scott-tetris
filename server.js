const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const path = require('path');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });
const PORT = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, 'public')));

// ── State ──
const players = new Map();
const rooms = new Map();

function uid() { return crypto.randomBytes(4).toString('hex'); }

function send(ws, msg) {
  if (ws.readyState === 1) ws.send(JSON.stringify(msg));
}

function broadcast(roomId, msg, excludeId) {
  const room = rooms.get(roomId);
  if (!room) return;
  const data = JSON.stringify(msg);
  for (const [id, p] of room.players) {
    if (id !== excludeId && p.ws.readyState === 1) p.ws.send(data);
  }
}

function roomInfo(room) {
  return {
    id: room.id, mode: room.mode, state: room.state, host: room.host,
    maxPlayers: room.mode === 'duel' ? 2 : 3,
    players: Array.from(room.players.values()).map(p => ({
      id: p.id, username: p.username, ready: p.ready, alive: p.alive
    }))
  };
}

function roomList() {
  const list = [];
  for (const [, room] of rooms) {
    list.push({
      id: room.id, mode: room.mode, state: room.state,
      playerCount: room.players.size,
      maxPlayers: room.mode === 'duel' ? 2 : 3,
      names: Array.from(room.players.values()).map(p => p.username)
    });
  }
  return list;
}

function broadcastRoomList() {
  const data = JSON.stringify({ type: 'room_list', rooms: roomList() });
  for (const [ws, p] of players) {
    if (!p.roomId && ws.readyState === 1) ws.send(data);
  }
}

function cleanupRoom(roomId) {
  const room = rooms.get(roomId);
  if (!room) return;
  if (room.players.size === 0) {
    if (room.cdTimer) clearInterval(room.cdTimer);
    rooms.delete(roomId);
    broadcastRoomList();
  }
}

function checkGameEnd(roomId) {
  const room = rooms.get(roomId);
  if (!room || room.state !== 'playing') return;
  const alive = Array.from(room.players.values()).filter(p => p.alive);
  if (alive.length <= 1) {
    room.state = 'finished';
    const w = alive[0] || null;
    broadcast(roomId, {
      type: 'game_end',
      winnerId: w ? w.id : null,
      winnerName: w ? w.username : 'Nobody'
    });
  }
}

function startCountdown(room) {
  room.state = 'countdown';
  let count = 3;
  broadcast(room.id, { type: 'countdown', seconds: count });
  room.cdTimer = setInterval(() => {
    count--;
    if (count > 0) {
      broadcast(room.id, { type: 'countdown', seconds: count });
    } else {
      clearInterval(room.cdTimer);
      room.cdTimer = null;
      room.state = 'playing';
      for (const p of room.players.values()) p.alive = true;
      broadcast(room.id, { type: 'game_start' });
    }
  }, 1000);
}

function handleLeave(ws) {
  const player = players.get(ws);
  if (!player || !player.roomId) return;
  const room = rooms.get(player.roomId);
  if (room) {
    room.players.delete(player.id);
    if (room.host === player.id && room.players.size > 0) {
      room.host = room.players.keys().next().value;
    }
    broadcast(room.id, { type: 'player_left', playerId: player.id, username: player.username });
    broadcast(room.id, { type: 'room_update', room: roomInfo(room) });
    if (room.state === 'playing') checkGameEnd(room.id);
    cleanupRoom(room.id);
  }
  player.roomId = null;
  broadcastRoomList();
}

// ── WebSocket ──
wss.on('connection', (ws) => {
  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {
      case 'login': {
        const id = uid();
        players.set(ws, { id, username: msg.username, roomId: null, ws });
        send(ws, { type: 'login_success', playerId: id });
        send(ws, { type: 'room_list', rooms: roomList() });
        break;
      }
      case 'create_room': {
        const p = players.get(ws);
        if (!p || p.roomId) return;
        const mode = msg.mode === 'trio' ? 'trio' : 'duel';
        const room = {
          id: uid(), mode, state: 'waiting', host: p.id,
          players: new Map(), cdTimer: null
        };
        room.players.set(p.id, {
          id: p.id, username: p.username, ws, ready: false, alive: true
        });
        p.roomId = room.id;
        rooms.set(room.id, room);
        send(ws, { type: 'room_joined', room: roomInfo(room), you: p.id });
        broadcastRoomList();
        break;
      }
      case 'join_room': {
        const p = players.get(ws);
        if (!p || p.roomId) return;
        const room = rooms.get(msg.roomId);
        if (!room) { send(ws, { type: 'error', message: 'Room not found' }); return; }
        if (room.state !== 'waiting') { send(ws, { type: 'error', message: 'Game in progress' }); return; }
        const max = room.mode === 'duel' ? 2 : 3;
        if (room.players.size >= max) { send(ws, { type: 'error', message: 'Room full' }); return; }
        room.players.set(p.id, {
          id: p.id, username: p.username, ws, ready: false, alive: true
        });
        p.roomId = room.id;
        broadcast(room.id, { type: 'room_update', room: roomInfo(room) });
        send(ws, { type: 'room_joined', room: roomInfo(room), you: p.id });
        broadcastRoomList();
        break;
      }
      case 'leave_room': {
        handleLeave(ws);
        send(ws, { type: 'room_list', rooms: roomList() });
        break;
      }
      case 'ready': {
        const p = players.get(ws);
        if (!p || !p.roomId) return;
        const room = rooms.get(p.roomId);
        if (!room || room.state !== 'waiting') return;
        const rp = room.players.get(p.id);
        if (rp) rp.ready = !rp.ready;
        broadcast(room.id, { type: 'room_update', room: roomInfo(room) });
        const all = Array.from(room.players.values()).every(x => x.ready);
        if (all && room.players.size >= 2) startCountdown(room);
        break;
      }
      case 'board_update': {
        const p = players.get(ws);
        if (!p || !p.roomId) return;
        broadcast(p.roomId, {
          type: 'opponent_update', playerId: p.id, username: p.username,
          board: msg.board, currentPiece: msg.currentPiece,
          score: msg.score, lines: msg.lines, level: msg.level
        }, p.id);
        break;
      }
      case 'send_garbage': {
        const p = players.get(ws);
        if (!p || !p.roomId) return;
        const room = rooms.get(p.roomId);
        if (!room || room.state !== 'playing') return;
        for (const [id, rp] of room.players) {
          if (id !== p.id && rp.alive) {
            send(rp.ws, { type: 'receive_garbage', lines: msg.lines, from: p.username });
          }
        }
        break;
      }
      case 'game_over': {
        const p = players.get(ws);
        if (!p || !p.roomId) return;
        const room = rooms.get(p.roomId);
        if (!room) return;
        const rp = room.players.get(p.id);
        if (rp) rp.alive = false;
        broadcast(p.roomId, { type: 'player_died', playerId: p.id, username: p.username });
        checkGameEnd(p.roomId);
        break;
      }
      case 'restart_room': {
        const p = players.get(ws);
        if (!p || !p.roomId) return;
        const room = rooms.get(p.roomId);
        if (!room || room.host !== p.id) return;
        room.state = 'waiting';
        for (const rp of room.players.values()) { rp.ready = false; rp.alive = true; }
        broadcast(room.id, { type: 'room_update', room: roomInfo(room) });
        break;
      }
      case 'refresh_rooms': {
        send(ws, { type: 'room_list', rooms: roomList() });
        break;
      }
    }
  });

  ws.on('close', () => {
    handleLeave(ws);
    players.delete(ws);
  });
});

server.listen(PORT, () => {
  console.log(`\n  SCOTT server running at http://localhost:${PORT}\n`);
});
