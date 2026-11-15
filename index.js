const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const { WebcastPushConnection } = require('tiktok-live-connector');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.static('public'));

const tiktokUsername = "Alkon191"; 

let tiktokLiveConnection = new WebcastPushConnection(tiktokUsername);
let connectedClients = new Set();

wss.on('connection', (ws) => {
    connectedClients.add(ws);
    console.log('Оверлей подключен к трансляции');

    ws.on('close', () => {
        connectedClients.delete(ws);
    });
});

function broadcast(data) {
    const message = JSON.stringify(data);
    for (const client of connectedClients) {
        if (client.readyState === 1) {
            client.send(message);
        }
    }
}

function connectTikTok() {
    tiktokLiveConnection.connect().then(state => {
        console.log(`Подключено к стриму @${tiktokUsername} (Комната: ${state.roomId})`);
    }).catch(err => {
        console.error('Ошибка подключения к стриму (повтор через 10 сек):', err);
        setTimeout(connectTikTok, 10000);
    });
}

tiktokLiveConnection.on('chat', (data) => {
    console.log(`${data.uniqueId}: ${data.comment}`);
    
    if (data.comment.toLowerCase().includes('привет')) {
        broadcast({
            type: 'chat_reply',
            user: data.uniqueId,
            comment: data.comment,
            text: `Приветик, ${data.uniqueId}! Рады видеть на стриме :)`
        });
    }
});

tiktokLiveConnection.on('like', (data) => {
    broadcast({
        type: 'like',
        user: data.uniqueId,
        likesCount: data.likeCount,
        totalLikes: data.totalLikeCount
    });
});

tiktokLiveConnection.on('gift', (data) => {
    if (data.giftType === 1 && !data.repeatEnd) {
        return;
    }
    console.log(`Подарок от ${data.uniqueId}: ${data.giftName} (х${data.repeatCount})`);
    
    broadcast({
        type: 'gift',
        user: data.uniqueId,
        avatar: data.profilePictureUrl || 'https://i.imgur.com/8Km9tLL.png',
        giftName: data.giftName,
        count: data.repeatCount
    });
});

tiktokLiveConnection.on('disconnect', () => {
    console.log('Стрим завершился или потеряна связь. Переподключение...');
    setTimeout(connectTikTok, 10000);
});

connectTikTok();

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Сервер запущен на порту ${PORT}`);
});
