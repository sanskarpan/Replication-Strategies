import { Elysia } from "elysia";
import { cors } from "@elysiajs/cors";

const BACKEND = process.env.BACKEND ?? "http://localhost:8080";
const BACKEND_WS = BACKEND.replace(/^http/, "ws");

// Per-connection upstream sockets for the /ws proxy, keyed by the client ws id.
const upstreams = new Map<string, WebSocket>();

// Bundle the client entrypoint so the browser receives resolvable JavaScript.
// Serving raw main.ts would ship bare imports ("d3", "./api/client") that no
// browser can resolve — the page would render nothing. Bundled lazily and cached.
let bundlePromise: Promise<string> | null = null;
async function getBundle(): Promise<string> {
  if (!bundlePromise) {
    bundlePromise = Bun.build({ entrypoints: ["src/main.ts"], target: "browser" }).then(
      async (b) => {
        if (!b.success) {
          console.error("main.ts bundle failed:", b.logs);
          throw new AggregateError(b.logs, "bundle failed");
        }
        return await b.outputs[0].text();
      }
    );
  }
  return bundlePromise;
}

const app = new Elysia()
  .use(cors())
  // Proxy all /api/* requests to Go backend
  .all("/api/*", async ({ request, params }) => {
    const url = request.url.replace(/^.*\/api/, `${BACKEND}/api`);
    // Forward only safe headers. Do NOT spread the incoming Host/Connection/
    // Content-Length headers — they belong to the client→BFF hop and corrupt the
    // upstream request (wrong Host, mismatched length).
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    const auth = request.headers.get("authorization");
    if (auth) headers["Authorization"] = auth;
    const init: RequestInit = {
      method: request.method,
      headers,
    };
    // Forward body for non-GET requests
    if (request.method !== "GET" && request.method !== "HEAD") {
      init.body = await request.text();
    }
    const resp = await fetch(url, init);
    const body = await resp.text();
    return new Response(body, {
      status: resp.status,
      headers: { "Content-Type": resp.headers.get("Content-Type") || "application/json" },
    });
  })
  // Serve static files
  .get("/", () => Bun.file("src/index.html"))
  .get("/main.js", async () => {
    try {
      return new Response(await getBundle(), {
        headers: { "Content-Type": "text/javascript" },
      });
    } catch (e) {
      return new Response(`console.error(${JSON.stringify(String(e))})`, {
        status: 500,
        headers: { "Content-Type": "text/javascript" },
      });
    }
  })
  .get("/styles.css", () => Bun.file("src/styles/main.css"))
  // Browsers auto-request /favicon.ico; answer 204 so it isn't a console 404.
  .get("/favicon.ico", () => new Response(null, { status: 204 }))
  // Proxy the WebSocket event stream to the backend so the browser can stay
  // single-origin (ws://<bff>/ws) instead of hardcoding the backend port.
  .ws("/ws", {
    open(ws) {
      const up = new WebSocket(`${BACKEND_WS}/ws`);
      upstreams.set(ws.id, up);
      up.addEventListener("message", (ev) => {
        try { ws.send(ev.data as string); } catch {}
      });
      up.addEventListener("close", () => { try { ws.close(); } catch {} });
      up.addEventListener("error", () => { try { ws.close(); } catch {} });
    },
    message(ws, message) {
      const up = upstreams.get(ws.id);
      if (up && up.readyState === WebSocket.OPEN) {
        up.send(typeof message === "string" ? message : JSON.stringify(message));
      }
    },
    close(ws) {
      upstreams.get(ws.id)?.close();
      upstreams.delete(ws.id);
    },
  })
  .listen(3001);

console.log(`BFF running at http://localhost:3001`);
