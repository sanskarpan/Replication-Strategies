import { Elysia } from "elysia";
import { cors } from "@elysiajs/cors";

const BACKEND = process.env.BACKEND ?? "http://localhost:8080";
const BACKEND_WS = BACKEND.replace(/^http/, "ws");

// Per-connection upstream sockets for the /ws proxy, keyed by the client ws id.
const upstreams = new Map<string, WebSocket>();

// Bundle the client entrypoint so the browser receives resolvable JavaScript.
// Serving raw main.ts would ship bare imports ("d3", "./api/client") that no
// browser can resolve — the page would render nothing.
//
// In dev (NODE_ENV !== "production"), the cache is invalidated when any file under
// src/ changes, so client edits show up on refresh without a server restart. In prod
// it is bundled once and cached forever.
const IS_PROD = process.env.NODE_ENV === "production";
let bundlePromise: Promise<string> | null = null;
let bundledMtime = 0;

async function srcMtime(): Promise<number> {
  // Newest mtime across the client source tree.
  const { readdir, stat } = await import("node:fs/promises");
  let newest = 0;
  async function walk(dir: string) {
    for (const e of await readdir(dir, { withFileTypes: true })) {
      const p = `${dir}/${e.name}`;
      if (e.isDirectory()) await walk(p);
      else newest = Math.max(newest, (await stat(p)).mtimeMs);
    }
  }
  await walk("src");
  return newest;
}

async function buildBundle(): Promise<string> {
  const b = await Bun.build({ entrypoints: ["src/main.ts"], target: "browser" });
  if (!b.success) {
    console.error("main.ts bundle failed:", b.logs);
    throw new AggregateError(b.logs, "bundle failed");
  }
  return await b.outputs[0].text();
}

async function getBundle(): Promise<string> {
  if (!IS_PROD) {
    const mtime = await srcMtime();
    if (mtime > bundledMtime) {
      bundledMtime = mtime;
      bundlePromise = buildBundle();
    }
  }
  if (!bundlePromise) bundlePromise = buildBundle();
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
