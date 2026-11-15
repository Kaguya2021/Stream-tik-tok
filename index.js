const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const tiktok = require('tiktok-live-connector');

// В tiktok-live-connector v2 класс называется TikTokLiveConnection,
// в v1 — WebcastPushConnection. Поддерживаем оба варианта.
const ConnectionClass = tiktok.TikTokLiveConnection || tiktok.WebcastPushConnection;
if (typeof ConnectionClass !== 'function') {
    console.error('Не найден класс подключения в tiktok-live-connector. Экспорты:', Object.keys(tiktok));
    process.exit(1);
}

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.static('public'));

const tiktokUsername = process.env.TIKTOK_USERNAME || "Alkon191";
const DEFAULT_AVATAR = 'https://i.imgur.com/8Km9tLL.png';

let connectedClients = new Set();
let tiktokLiveConnection = null;
let reconnectTimer = null;
// Сессия твинка (необязательно): нужна, чтобы бот писал в чат TikTok
const SESSION_ID = process.env.TIKTOK_SESSION_ID || '';
const TARGET_IDC = process.env.TIKTOK_TARGET_IDC || '';
const CAN_SEND = Boolean(SESSION_ID && TARGET_IDC);
const REPLY_COOLDOWN_MS = 60000; // не отвечать одному человеку чаще раза в минуту
const lastReplyAt = new Map();
let lastSendAt = 0;
const RETRY_MS = 30000; // слишком частые попытки приводят к rate-limit
const MANUAL_ROOM_ID = process.env.TIKTOK_ROOM_ID || undefined; // можно задать вручную

// Последние сообщения чата — показываем оверлею сразу при (пере)подключении
const chatHistory = [];
const HISTORY_LIMIT = 30;

wss.on('connection', (ws) => {
    connectedClients.add(ws);
    for (const msg of chatHistory) ws.send(JSON.stringify(msg));
    console.log('Оверлей подключен к трансляции');
    ws.on('close', () => connectedClients.delete(ws));
    ws.on('error', () => connectedClients.delete(ws));
});

function broadcast(data) {
    const message = JSON.stringify(data);
    for (const client of connectedClients) {
        if (client.readyState === 1) {
            client.send(message);
        }
    }
}

// --- Хелперы, совместимые с v1 и v2 (в v2 данные пользователя лежат в data.user) ---
const getUser = (d) => d.uniqueId || (d.user && d.user.uniqueId) || 'unknown';

function getAvatar(d) {
    if (d.profilePictureUrl) return d.profilePictureUrl;
    const pic = d.user && d.user.profilePicture;
    if (pic) {
        if (typeof pic === 'string') return pic;
        if (Array.isArray(pic.urls) && pic.urls[0]) return pic.urls[0];
        if (Array.isArray(pic.url) && pic.url[0]) return pic.url[0];
    }
    return DEFAULT_AVATAR;
}

const getGiftName = (d) => d.giftName || (d.giftDetails && d.giftDetails.giftName) || 'Подарок';
const getGiftType = (d) => (d.giftType !== undefined ? d.giftType : d.giftDetails && d.giftDetails.giftType);

// Событие может называться по-разному в v1/v2 — подписываемся на оба
function on(conn, names, handler) {
    for (const n of [].concat(names)) conn.on(n, handler);
}

function scheduleReconnect() {
    if (reconnectTimer) return;
    reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        connectTikTok();
    }, RETRY_MS);
}

function createConnection() {
    // Второй аргумент (options) обязателен в v2 — без него конструктор падает
    const conn = new ConnectionClass(tiktokUsername, {
        processInitialData: false,   // не повторять старые события при подключении
        enableExtendedGiftInfo: false,
        ...(process.env.EULER_API_KEY ? { signApiKey: process.env.EULER_API_KEY } : {})
    });

    if (CAN_SEND && conn.webClient && conn.webClient.cookieJar
        && typeof conn.webClient.cookieJar.setSessionBundle === 'function') {
        Promise.resolve(conn.webClient.cookieJar.setSessionBundle({
            type: 'cookie',
            value: { sessionId: SESSION_ID, ttTargetIdc: TARGET_IDC }
        })).then(() => console.log('Сессия твинка применена — ответы в чат включены'))
          .catch(e => console.error('Не удалось применить сессию твинка:', (e && e.message) || e));
    } else if (CAN_SEND) {
        console.warn('TIKTOK_SESSION_ID задан, но эта версия библиотеки не поддерживает сессию.');
    }

    on(conn, 'chat', (data) => {
        const user = getUser(data);
        const comment = String(data.comment || '');
        console.log(`${user}: ${comment}`);

        // Каждое сообщение из чата — на оверлей (ник + текст)
        if (comment.trim()) {
            const chatMsg = {
                type: 'chat',
                user,
                nickname: (data.user && data.user.nickname) || data.nickname || user,
                comment,
                avatar: getAvatar(data)
            };
            chatHistory.push(chatMsg);
            if (chatHistory.length > HISTORY_LIMIT) chatHistory.shift();
            broadcast(chatMsg);
        }

        // Свои же ответы («Приветик, ...») игнорируем, чтобы бот не зациклился
        const isOwnReply = comment.startsWith('Приветик,');

        if (!isOwnReply && comment.toLowerCase().includes('привет')) {
            const text = `Приветик, ${user}! Рады видеть на стриме :)`;
            broadcast({ type: 'chat_reply', user, comment, text });

            // Ответ в сам чат TikTok от имени твинка
            const now = Date.now();
            if (CAN_SEND
                && now - (lastReplyAt.get(user) || 0) > REPLY_COOLDOWN_MS
                && now - lastSendAt > 3000
                && typeof conn.sendMessage === 'function') {
                lastReplyAt.set(user, now);
                lastSendAt = now;
                Promise.resolve(conn.sendMessage(text))
                    .catch(e => console.error('Не удалось отправить в чат TikTok:', (e && e.message) || e));
            }
        }
    });

    on(conn, 'like', (data) => {
        broadcast({
            type: 'like',
            user: getUser(data),
            likesCount: data.likeCount,
            totalLikes: data.totalLikeCount
        });
    });

    on(conn, 'gift', (data) => {
        if (getGiftType(data) === 1 && !data.repeatEnd) {
            return; // серия подарков ещё идёт — ждём конца
        }
        const giftName = getGiftName(data);
        console.log(`Подарок от ${getUser(data)}: ${giftName} (х${data.repeatCount})`);

        broadcast({
            type: 'gift',
            user: getUser(data),
            avatar: getAvatar(data),
            giftName,
            count: data.repeatCount
        });
    });

    // v1: 'disconnected' / 'streamEnd', v2: 'disconnected' / 'streamEnd'
    on(conn, ['disconnected', 'disconnect', 'streamEnd'], () => {
        console.log('Стрим завершился или потеряна связь. Переподключение...');
        scheduleReconnect();
    });

    // Чтобы необработанное событие 'error' не роняло процесс
    on(conn, 'error', (err) => {
        console.error('Ошибка TikTok соединения:', (err && (err.info || err.message)) || err);
        if (err && err.exception) console.error('  Детали:', err.exception.message || err.exception);
    });

    return conn;
}

function connectTikTok() {
    if (tiktokLiveConnection) {
        try { tiktokLiveConnection.disconnect(); } catch (_) {}
    }
    tiktokLiveConnection = createConnection();

    tiktokLiveConnection.connect(MANUAL_ROOM_ID).then(state => {
        console.log(`Подключено к стриму @${tiktokUsername} (Комната: ${state.roomId})`);
    }).catch(err => {
        console.error(`Ошибка подключения к стриму (повтор через ${RETRY_MS / 1000} сек):`, err && err.message ? err.message : err);
        // Подробности причины (v2 кладёт их в cause / exception)
        const cause = err && (err.cause || err.exception || err.exceptions);
        if (cause) console.error('  Причина:', cause.message || cause);
        scheduleReconnect();
    });
}

// Не даём сервису упасть из-за случайной ошибки библиотеки
process.on('unhandledRejection', (e) => console.error('unhandledRejection:', e));
process.on('uncaughtException', (e) => console.error('uncaughtException:', e));

connectTikTok();

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Сервер запущен на порту ${PORT}`);
});
