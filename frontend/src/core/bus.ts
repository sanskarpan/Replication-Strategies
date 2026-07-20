import { WSClient } from "../ws/client";

// bus is the single shared WebSocket event stream. Components subscribe via bus.on(type,
// handler) in their mount(); the bootstrap connects it once. Having one shared instance
// (instead of each module newing its own) is what lets many components react to the same
// event without opening multiple sockets.
export const bus = new WSClient();
