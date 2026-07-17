import type { SimEvent, EventType } from "../api/types";

type EventHandler = (evt: SimEvent) => void;

export class WSClient {
  private ws: WebSocket | null = null;
  private handlers: Map<EventType | "*", EventHandler[]> = new Map();
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private url: string;

  constructor(filter: EventType[] = []) {
    const filterParam = filter.length ? `?filter=${filter.join(",")}` : "";
    // Connect same-origin (the BFF), which proxies /ws to the backend. This works
    // regardless of the backend's port instead of hardcoding :8080.
    const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    this.url = `${proto}//${window.location.host}/ws${filterParam}`;
  }

  connect() {
    this.ws = new WebSocket(this.url);
    this.ws.onmessage = (e) => {
      try {
        const evt: SimEvent = JSON.parse(e.data);
        this.dispatch(evt);
      } catch {}
    };
    this.ws.onclose = () => {
      this.reconnectTimer = setTimeout(() => this.connect(), 2000);
    };
    this.ws.onerror = () => this.ws?.close();
  }

  disconnect() {
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.ws?.close();
  }

  on(type: EventType | "*", handler: EventHandler) {
    if (!this.handlers.has(type)) this.handlers.set(type, []);
    this.handlers.get(type)!.push(handler);
  }

  off(type: EventType | "*", handler: EventHandler) {
    const list = this.handlers.get(type);
    if (list) {
      const idx = list.indexOf(handler);
      if (idx !== -1) list.splice(idx, 1);
    }
  }

  private dispatch(evt: SimEvent) {
    const specific = this.handlers.get(evt.type) || [];
    const wildcard = this.handlers.get("*") || [];
    [...specific, ...wildcard].forEach((h) => h(evt));
  }
}
