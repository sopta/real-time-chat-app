import { api, StreamInOut } from "encore.dev/api";
import log from "encore.dev/log";

interface HandshakeRequest {
  id: string;
  username: string;
}

interface PostMessage {
  username: string;
  msg: string;
}

interface PresenceUser {
  id: string;
  username: string;
}

interface ServerEvent {
  type: "chat" | "presence";
  userID?: string;
  username?: string;
  msg?: string;
  users?: PresenceUser[];
}

interface Connection {
  username: string;
  stream: StreamInOut<PostMessage, ServerEvent>;
}

const connectedStreams: Map<string, Connection> = new Map();

async function broadcastPresence() {
  const users: PresenceUser[] = Array.from(connectedStreams, ([id, conn]) => ({
    id,
    username: conn.username,
  }));
  const event: ServerEvent = { type: "presence", users };
  for (const [key, conn] of connectedStreams) {
    try {
      await conn.stream.send(event);
    } catch (err) {
      connectedStreams.delete(key);
      log.error("error sending presence", err);
    }
  }
}

export const chat = api.streamInOut<HandshakeRequest, PostMessage, ServerEvent>(
  { expose: true, auth: false, path: "/chat" },
  async (handshake, stream) => {
    connectedStreams.set(handshake.id, { username: handshake.username, stream });
    log.info("user connected", handshake);
    await broadcastPresence();

    try {
      for await (const chatMessage of stream) {
        for (const [key, conn] of connectedStreams) {
          try {
            await conn.stream.send({
              type: "chat",
              userID: handshake.id,
              username: chatMessage.username,
              msg: chatMessage.msg,
            });
          } catch (err) {
            connectedStreams.delete(key);
            log.error("error sending", err);
          }
        }
      }
    } catch (err) {
      log.error("stream error", err);
    }

    connectedStreams.delete(handshake.id);
    await broadcastPresence();
  },
);
