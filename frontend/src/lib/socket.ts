import { Socket, Channel } from "phoenix";

let socket: Socket | null = null;

export function getSocket(): Socket {
  if (!socket) {
    socket = new Socket("/socket", {});
    socket.connect();
  }
  return socket;
}

export function joinChannel(topic: string): Channel {
  const ch = getSocket().channel(topic, {});
  ch.join().receive("error", (err) => {
    console.error(`[channel] failed to join ${topic}:`, err);
  });
  return ch;
}
