
const WebSocket = require("ws");
const http = require("http");

const PORT = process.env.PORT || 10000;
const MAX_MEMBERS = 5;
const MAX_PAYLOAD = 2 * 1024 * 1024;

const server = http.createServer((req, res) => {
    res.writeHead(200, {
        "Content-Type": "text/plain"
    });
    res.end("Share Map Server Online");
});

const wss = new WebSocket.Server({
    server,
    maxPayload: MAX_PAYLOAD
});

const rooms = new Map();

function sendJson(ws, data) {
    if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(data));
    }
}

function getRoom(ws) {
    if (!ws.room) return null;
    return rooms.get(ws.room) || null;
}

function broadcast(room, data, exclude = null) {
    for (const client of room.members) {
        if (
            client !== exclude &&
            client.readyState === WebSocket.OPEN
        ) {
            client.send(JSON.stringify(data));
        }
    }
}

function broadcastShareState(room) {
    for (const client of room.members) {
        sendJson(client, {
            type: "share_state",
            busy: room.sharer !== null,
            mine: room.sharer === client
        });
    }
}

function leaveRoom(ws) {
    const room = getRoom(ws);
    if (!room) return;

    if (room.sharer === ws) {
        room.sharer = null;
    }

    room.members.delete(ws);
    ws.room = null;

    if (room.members.size > 0) {
        broadcastShareState(room);
    } else {
        rooms.delete(room.name);
    }
}

wss.on("connection", (ws) => {
    ws.room = null;

    ws.on("message", (message, isBinary) => {
        const room = getRoom(ws);

        // Nhận và chuyển tiếp dữ liệu H.264 dạng Binary.
        if (isBinary) {
            if (!room || room.sharer !== ws) {
                return;
            }

            for (const client of room.members) {
                if (
                    client !== ws &&
                    client.readyState === WebSocket.OPEN
                ) {
                    client.send(message, {
                        binary: true
                    });
                }
            }

            return;
        }

        let data;

        try {
            data = JSON.parse(message.toString());
        } catch (error) {
            sendJson(ws, {
                type: "error",
                message: "Dữ liệu JSON không hợp lệ"
            });
            return;
        }

        switch (data.type) {

            case "join": {
                if (
                    typeof data.room !== "string" ||
                    !/^\d{6}$/.test(data.room)
                ) {
                    sendJson(ws, {
                        type: "error",
                        message: "Mã phòng không hợp lệ"
                    });
                    return;
                }

                leaveRoom(ws);

                let targetRoom = rooms.get(data.room);

                if (!targetRoom) {
                    targetRoom = {
                        name: data.room,
                        members: new Set(),
                        sharer: null
                    };

                    rooms.set(data.room, targetRoom);
                }

                if (targetRoom.members.size >= MAX_MEMBERS) {
                    sendJson(ws, {
                        type: "error",
                        message: "Phòng đã đủ 5 thiết bị"
                    });
                    return;
                }

                targetRoom.members.add(ws);
                ws.room = data.room;

                sendJson(ws, {
                    type: "joined",
                    room: data.room
                });

                broadcastShareState(targetRoom);
                break;
            }

            case "claim": {
                if (!room) return;

                if (
                    room.sharer === null ||
                    room.sharer === ws
                ) {
                    room.sharer = ws;

                    sendJson(ws, {
                        type: "share_granted"
                    });

                    broadcastShareState(room);
                } else {
                    sendJson(ws, {
                        type: "share_denied"
                    });
                }

                break;
            }

            case "release": {
                if (!room) return;

                if (room.sharer === ws) {
                    room.sharer = null;
                    broadcastShareState(room);
                }

                break;
            }

            case "map": {
                if (!room) return;

                if (
                    room.sharer !== null &&
                    room.sharer !== ws
                ) {
                    return;
                }

                for (const client of room.members) {
                    if (
                        client !== ws &&
                        client.readyState === WebSocket.OPEN
                    ) {
                        sendJson(client, {
                            type: "map",
                            image: data.image
                        });
                    }
                }

                break;
            }

            case "hide": {
                if (!room) return;

                broadcast(room, {
                    type: "hide"
                }, ws);

                break;
            }

            default: {
                sendJson(ws, {
                    type: "error",
                    message: "Lệnh không được hỗ trợ"
                });
            }
        }
    });

    ws.on("close", () => {
        leaveRoom(ws);
    });

    ws.on("error", (error) => {
        console.error("WebSocket error:", error.message);
    });
});

server.listen(PORT, "0.0.0.0", () => {
    console.log("Share Map Server running on port " + PORT);
});
