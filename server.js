
const WebSocket = require("ws");
const http = require("http");
const crypto = require("crypto");

const PORT = process.env.PORT || 10000;
const MAX_MEMBERS = 5;
const MAX_PAYLOAD = 256 * 1024;
const RECONNECT_TTL = 24 * 60 * 60 * 1000;

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
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(data));
    }
}

function getRoom(ws) {
    if (!ws.room) return null;
    return rooms.get(ws.room) || null;
}

function broadcast(room, data, exclude = null) {
    for (const client of room.members.values()) {
        if (client && client !== exclude) {
            sendJson(client, data);
        }
    }
}

function broadcastShareState(room) {
    for (const [id, client] of room.members.entries()) {
        if (!client) continue;

        sendJson(client, {
            type: "share_state",
            busy: room.sharerId !== null,
            mine: room.sharerId === id
        });
    }
}

function createRoom(name) {
    return {
        name,
        members: new Map(),
        memberTimers: new Map(),
        sharerId: null
    };
}

function removeMember(room, id) {
    if (!room || !room.members.has(id)) return;

    const timer = room.memberTimers.get(id);

    if (timer) {
        clearTimeout(timer);
        room.memberTimers.delete(id);
    }

    room.members.delete(id);

    if (room.sharerId === id) {
        room.sharerId = null;
    }

    broadcast(room, {
        type: "peer_left",
        id
    });

    if (room.members.size > 0) {
        broadcastShareState(room);
    } else {
        rooms.delete(room.name);
    }
}

function leaveRoom(ws) {
    const room = getRoom(ws);
    if (!room || !ws.id) return;

    const id = ws.id;

    if (room.members.get(id) === ws) {
        removeMember(room, id);
    }

    ws.room = null;
    ws.id = null;
}

function handleDisconnect(ws) {
    const room = getRoom(ws);
    if (!room || !ws.id) return;

    const id = ws.id;

    if (room.members.get(id) !== ws) return;

    room.members.set(id, null);

    broadcast(room, {
        type: "peer_left",
        id
    });

    broadcastShareState(room);

    const oldTimer = room.memberTimers.get(id);

    if (oldTimer) {
        clearTimeout(oldTimer);
    }

    const timer = setTimeout(() => {
        const currentRoom = rooms.get(room.name);

        if (
            currentRoom &&
            currentRoom.members.has(id) &&
            currentRoom.members.get(id) === null
        ) {
            removeMember(currentRoom, id);
        }
    }, RECONNECT_TTL);

    room.memberTimers.set(id, timer);

    ws.room = null;
    ws.id = null;
}

wss.on("connection", (ws) => {
    ws.room = null;
    ws.id = null;

    ws.on("message", (message, isBinary) => {

        // VIDEO H.264 BINARY RELAY
        if (isBinary) {
            const room = getRoom(ws);

            if (
                !room ||
                !ws.id ||
                room.sharerId !== ws.id ||
                room.members.get(ws.id) !== ws
            ) {
                return;
            }

            for (const [id, client] of room.members.entries()) {
                if (
                    id !== ws.id &&
                    client &&
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

        const room = getRoom(ws);

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

                if (
                    typeof data.deviceId !== "string" ||
                    !/^[a-zA-Z0-9-]{8,64}$/.test(data.deviceId)
                ) {
                    sendJson(ws, {
                        type: "error",
                        message: "ID thiết bị không hợp lệ"
                    });
                    return;
                }

                leaveRoom(ws);

                let targetRoom = rooms.get(data.room);

                if (!targetRoom) {
                    targetRoom = createRoom(data.room);
                    rooms.set(data.room, targetRoom);
                }

                const deviceId = data.deviceId;
                const existingDevice =
                    targetRoom.members.has(deviceId);

                if (
                    !existingDevice &&
                    targetRoom.members.size >= MAX_MEMBERS
                ) {
                    sendJson(ws, {
                        type: "error",
                        message: "Phòng đã đủ 5 thiết bị"
                    });
                    return;
                }

                const oldTimer =
                    targetRoom.memberTimers.get(deviceId);

                if (oldTimer) {
                    clearTimeout(oldTimer);
                    targetRoom.memberTimers.delete(deviceId);
                }

                const existingMembers = [];

                for (const [id, member] of targetRoom.members.entries()) {
                    if (id !== deviceId && member) {
                        existingMembers.push({ id });
                    }
                }

                targetRoom.members.set(deviceId, ws);

                ws.id = deviceId;
                ws.room = data.room;

                sendJson(ws, {
                    type: "joined",
                    room: data.room,
                    id: deviceId,
                    members: existingMembers,
                    restored: existingDevice,
                    sharing: targetRoom.sharerId === deviceId
                });

                broadcast(targetRoom, {
                    type: "peer_joined",
                    id: deviceId,
                    restored: existingDevice
                }, ws);

                if (targetRoom.sharerId === deviceId) {
                    sendJson(ws, {
                        type: "share_granted"
                    });
                }

                broadcastShareState(targetRoom);
                break;
            }

            case "claim": {
                if (!room) {
                    sendJson(ws, {
                        type: "error",
                        message: "Bạn chưa vào phòng"
                    });
                    return;
                }

                if (
                    room.sharerId === null ||
                    room.sharerId === ws.id
                ) {
                    room.sharerId = ws.id;

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

                if (room.sharerId === ws.id) {
                    room.sharerId = null;

                    broadcast(room, {
                        type: "share_stopped",
                        id: ws.id
                    }, ws);

                    broadcastShareState(room);
                }

                break;
            }

            case "signal": {
                if (!room) {
                    sendJson(ws, {
                        type: "error",
                        message: "Bạn chưa vào phòng"
                    });
                    return;
                }

                const targetId = data.to;
                const target = room.members.get(targetId);

                if (!target || target === ws) {
                    sendJson(ws, {
                        type: "error",
                        message: "Thiết bị nhận không tồn tại hoặc đang ngoại tuyến"
                    });
                    return;
                }

                if (
                    !data.signal ||
                    typeof data.signal !== "object" ||
                    !["offer", "answer", "ice"].includes(
                        data.signal.type
                    )
                ) {
                    sendJson(ws, {
                        type: "error",
                        message: "Gói signaling không hợp lệ"
                    });
                    return;
                }

                if (
                    data.signal.type === "offer" &&
                    room.sharerId !== ws.id
                ) {
                    sendJson(ws, {
                        type: "error",
                        message: "Chỉ thiết bị đang chia sẻ được gửi Offer"
                    });
                    return;
                }

                sendJson(target, {
                    type: "signal",
                    from: ws.id,
                    signal: data.signal
                });

                break;
            }

            case "hide": {
                if (!room) return;

                if (room.sharerId !== ws.id) {
                    sendJson(ws, {
                        type: "error",
                        message: "Bạn không có quyền chia sẻ"
                    });
                    return;
                }

                broadcast(room, {
                    type: "hide",
                    id: ws.id
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
        handleDisconnect(ws);
    });

    ws.on("error", (error) => {
        console.error("WebSocket error:", error.message);
    });
});

server.listen(PORT, "0.0.0.0", () => {
    console.log(
        "Share Map Server running on port " + PORT
    );
});
{
        handleDisconnect(ws);
    });

    ws.on("error", (error) => {
        console.error("WebSocket error:", error.message);
    });
});

server.listen(PORT, "0.0.0.0", () => {
    console.log(
        "Share Map Signaling Server running on port " + PORT
    );
});
