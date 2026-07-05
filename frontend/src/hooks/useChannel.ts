import { useEffect, useRef, useCallback } from "react";
import { Channel } from "phoenix";
import { joinChannel } from "../lib/socket";

type Handler = (payload: unknown) => void;

export function useChannel(
  topic: string,
  handlers: Record<string, Handler>
) {
  const channelRef = useRef<Channel | null>(null);
  const handlersRef = useRef(handlers);
  handlersRef.current = handlers;

  useEffect(() => {
    const ch = joinChannel(topic);
    channelRef.current = ch;

    const refs = Object.entries(handlersRef.current).map(([event, fn]) =>
      ch.on(event, fn)
    );

    return () => {
      refs.forEach((ref, i) => {
        const event = Object.keys(handlersRef.current)[i];
        ch.off(event, ref);
      });
      ch.leave();
      channelRef.current = null;
    };
  }, [topic]);

  const push = useCallback((event: string, payload: unknown = {}) => {
    channelRef.current?.push(event, payload);
  }, []);

  return { push };
}
