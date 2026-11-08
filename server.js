
const WebSocket = require("ws");
const http = require("http");

const server = http.createServer((req, res) => {
    res.writeHead(200);
    res.end("Share Map Server Online");
});

const wss = new WebSocket.Server({ server });
const rooms = new Map();

function send(ws, data) {
    if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(data));
    }
}

function broadcastState(room) {
    const state = rooms.get(room);
    if (!state) return;

    for (const client of state.members) {
        send(client, {
            type: "share_state",
            busy: state.sharer !== null,
            mine: state.sharer === client
        });
    }
}

function leaveRoom(ws) {
    const room = ws.room;
    if (!room || !rooms.has(room)) return;

    const state = rooms.get(room);
    state.members.delete(ws);

    if (state.sharer === ws) {
        state.sharer = null;
    }

    ws.room = null;

    if (state.members.size === 0) {
        rooms.delete(room);
    } else {
        broadcastState(room);
    }
}

wss.on("connection", (ws) => {
    ws.room = null;

    ws.on("message", (message) => {
        try {
            const data = JSON.parse(message.toString());

            if (data.type === "join") {
                if (!/^\d{6}$/.test(data.room)) {
                    send(ws, {
                        type: "error",
                        message: "Mã phòng không hợp lệ"
                    });
                    return;
                }

                leaveRoom(ws);

                if (!rooms.has(data.room)) {
                    rooms.set(data.room, {
                        members: new Set(),
                        sharer: null
                    });
                }

                const state = rooms.get(data.room);

                if (state.members.size >= 3) {
                    send(ws, {
                        type: "error",
                        message: "Phòng đã đủ 3 người"
                    });

                    return;
                }

                ws.room = data.room;
                state.members.add(ws);

                send(ws, {
                    type: "joined",
                    room: data.room
                });

                broadcastState(data.room);
                return;
            }

            const room = ws.room;
            if (!room || !rooms.has(room)) return;

            const state = rooms.get(room);

            if (data.type === "claim") {
                if (state.sharer === null) {
                    state.sharer = ws;

                    send(ws, { type: "share_granted" });
                    broadcastState(room);
                } else if (state.sharer === ws) {
                    send(ws, { type: "share_granted" });
                } else {
                    send(ws, { type: "share_denied" });
                }

                return;
            }

            if (data.type === "release") {
                if (state.sharer === ws) {
                    state.sharer = null;
                    broadcastState(room);
                }

                return;
            }

            if (data.type === "map") {
                if (state.sharer !== ws) return;
                if (typeof data.image !== "string") return;

                for (const client of state.members) {
                    if (client !== ws) {
                        send(client, {
                            type: "map",
                            image: data.image
                        });
                    }
                }

                return;
            }

            if (data.type === "hide") {
                if (state.sharer !== ws) return;

                for (const client of state.members) {
                    if (client !== ws) {
                        send(client, { type: "hide" });
                    }
                }
            }

        } catch (error) {
            send(ws, {
                type: "error",
                message: "Dữ liệu không hợp lệ"
            });
        }
    });

    ws.on("close", () => {
        leaveRoom(ws);
    });

    ws.on("error", () => {
        leaveRoom(ws);
    });
});

const PORT = process.env.PORT || 10000;

server.listen(PORT, "0.0.0.0", () => {
    console.log("Share Map running on port " + PORT);
});
