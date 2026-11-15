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

        if (comment.toLowerCase().includes('привет')) {
            broadcast({
                type: 'chat_reply',
                user,
                comment,
                text: `Приветик, ${user}! Рады видеть на стриме :)`
            });
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
