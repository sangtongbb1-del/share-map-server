
const WebSocket = require("ws");
const http = require("http");

const server = http.createServer((req, res) => {
    res.writeHead(200);
    res.end("Share Map Server Online");
});

const wss = new WebSocket.Server({ server });
const rooms = new Map();

wss.on("connection", (ws) => {
    let room = null;

    ws.on("message", (message) => {
        try {
            const data = JSON.parse(message.toString());

            if (data.type === "join") {
                if (!/^\d{6}$/.test(data.room)) {
                    ws.send(JSON.stringify({
                        type: "error",
                        message: "Mã phòng không hợp lệ"
                    }));
                    return;
                }

                room = data.room;

                if (!rooms.has(room)) {
                    rooms.set(room, new Set());
                }

                rooms.get(room).add(ws);

                ws.send(JSON.stringify({
                    type: "joined",
                    room
                }));

                return;
            }

            if (
                (data.type === "map" && typeof data.image === "string") ||
                data.type === "hide"
            ) {
                if (!room || !rooms.has(room)) return;

                const members = rooms.get(room);

                const messageToSend = data.type === "map"
                    ? JSON.stringify({
                        type: "map",
                        image: data.image
                    })
                    : JSON.stringify({
                        type: "hide"
                    });

                for (const client of members) {
                    if (
                        client !== ws &&
                        client.readyState === WebSocket.OPEN
                    ) {
                        client.send(messageToSend);
                    }
                }
            }

        } catch (error) {
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({
                    type: "error",
                    message: "Dữ liệu không hợp lệ"
                }));
            }
        }
    });

    ws.on("close", () => {
        if (room && rooms.has(room)) {
            const members = rooms.get(room);
            members.delete(ws);

            if (members.size === 0) {
                rooms.delete(room);
            }
        }
    });
});

const PORT = process.env.PORT || 10000;

server.listen(PORT, "0.0.0.0", () => {
    console.log("Share Map running on port " + PORT);
});
