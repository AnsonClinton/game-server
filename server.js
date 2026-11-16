'use strict';
/*
 * 单文件联机服务器（Node + WebSocket）
 * 适用：Android WebView 壳 + APK 本地 assets 里的 Web 游戏
 *
 * Render 部署只需这一个文件，配置填：
 *   Build Command: npm install ws
 *   Start Command: node server.js
 *
 * 客户端连接：Net.init("wss://你的服务名.onrender.com")
 */

const http = require('http');
const { WebSocketServer } = require('ws');

/** @type {Map<number, import('ws').WebSocket>} */
const clients = new Map();
let nextId = 1;

// ---------- HTTP：只留健康检查（给 UptimeRobot 保活用） ----------
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, online: clients.size, uptime: Math.round(process.uptime()) }));
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('game server ok — connect via wss://' + (req.headers.host || '') + '\n');
});

// ---------- WebSocket ----------
const wss = new WebSocketServer({ server });

const roomOf = (ws) => ws.room || 'lobby';

function send(ws, obj) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}
function broadcast(obj, exceptWs) {
  const data = JSON.stringify(obj);
  for (const ws of clients.values()) {
    if (ws !== exceptWs && ws.readyState === ws.OPEN) ws.send(data);
  }
}
function roomBroadcast(room, obj, exceptWs) {
  const data = JSON.stringify(obj);
  for (const ws of clients.values()) {
    if (ws !== exceptWs && roomOf(ws) === room && ws.readyState === ws.OPEN) ws.send(data);
  }
}
function onlineList() {
  return Array.from(clients.entries()).map(([id, ws]) => ({
    id, name: ws.name || ('玩家' + id), room: roomOf(ws)
  }));
}
function pushOnline() { broadcast({ type: 'online', list: onlineList() }); }

wss.on('connection', (ws) => {
  const id = nextId++;
  ws.id = id;
  ws.name = '玩家' + id;
  ws.room = 'lobby';
  ws.isAlive = true;
  clients.set(id, ws);

  send(ws, { type: 'welcome', id, name: ws.name });
  broadcast({ type: 'system', text: `${ws.name} 上线了` }, ws);
  pushOnline();

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    if (!msg || typeof msg.type !== 'string') return;

    switch (msg.type) {
      case 'setname': {
        const name = String(msg.name || '').slice(0, 20).trim();
        if (name) ws.name = name;
        broadcast({ type: 'system', text: `玩家${id} 改名为 ${ws.name}` });
        pushOnline();
        break;
      }
      case 'join': {
        const room = String(msg.room || 'lobby').slice(0, 24).trim() || 'lobby';
        const old = ws.room;
        roomBroadcast(old, { type: 'system', text: `${ws.name} 离开了房间 ${old}` }, ws);
        ws.room = room;
        send(ws, { type: 'joined', room });
        roomBroadcast(room, { type: 'system', text: `${ws.name} 进入了房间 ${room}` }, ws);
        pushOnline();
        break;
      }
      case 'chat': {
        const text = String(msg.text || '').slice(0, 500);
        if (text) roomBroadcast(ws.room, { type: 'chat', from: id, name: ws.name, text });
        break;
      }
      case 'whisper': {
        const to = clients.get(Number(msg.to));
        const text = String(msg.text || '').slice(0, 500);
        if (!text) return;
        if (to && to.readyState === to.OPEN) {
          send(to, { type: 'whisper', from: id, name: ws.name, text });
          send(ws, { type: 'whisper-sent', to: to.name, text });
        } else {
          send(ws, { type: 'error', text: '对方不在线' });
        }
        break;
      }
      case 'game': {
        // 游戏同步：转发给同房间其他人（不含自己）
        roomBroadcast(ws.room, { type: 'game', from: id, data: msg.data }, ws);
        break;
      }
      default:
        send(ws, { type: 'error', text: '未知消息类型: ' + msg.type });
    }
  });

  ws.on('close', () => {
    clients.delete(id);
    broadcast({ type: 'system', text: `${ws.name} 离开了` });
    pushOnline();
  });
  ws.on('error', () => { clients.delete(id); pushOnline(); });
});

// 心跳：清理断线连接（浏览器/WebView 会自动回 pong）
const timer = setInterval(() => {
  for (const [id, ws] of clients) {
    if (!ws.isAlive) { ws.terminate(); clients.delete(id); continue; }
    ws.isAlive = false;
    ws.ping();
  }
  if (clients.size > 0) pushOnline();
}, 30000);
wss.on('close', () => clearInterval(timer));

const port = process.env.PORT || 8080;
server.listen(port, '0.0.0.0', () => {
  console.log('✅ 联机服务器已启动，端口 ' + port);
});
