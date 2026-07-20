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
// Opt-in live-reload (HMR-lite): DEV_HMR=1 watches src/ and pushes a reload to the
// browser on any change. Off by default so the Playwright E2E is never affected.
const DEV_HMR = !!process.env.DEV_HMR;
let bundlePromise: Promise<string> | null = null;
let bundledMtime = 0;

// Live-reload client sockets + a small client snippet injected into index.html in DEV_HMR.
const liveReloadClients = new Set<{ send: (s: string) => void }>();
const LIVE_RELOAD_SNIPPET = `
<script>(function(){
  try {
    var proto = location.protocol === "https:" ? "wss:" : "ws:";
    var ws = new WebSocket(proto + "//" + location.host + "/__livereload");
    ws.onmessage = function(e){ if (e.data === "reload") location.reload(); };
    ws.onclose = function(){ setTimeout(function(){ location.reload(); }, 1000); };
  } catch (e) {}
})();</script>`;

if (DEV_HMR) {
  const { watch } = await import("node:fs");
  let debounce: ReturnType<typeof setTimeout> | null = null;
  watch("src", { recursive: true }, () => {
    if (debounce) clearTimeout(debounce);
    debounce = setTimeout(() => {
      bundledMtime = 0; // force a rebuild on next request
      for (const c of liveReloadClients) {
        try { c.send("reload"); } catch {}
      }
    }, 120);
  });
  console.log("live-reload watching src/ (DEV_HMR=1)");
}

// serveIndex returns index.html, injecting the live-reload client in DEV_HMR mode.
async function serveIndex(): Promise<Response> {
  if (!DEV_HMR) return new Response(Bun.file("src/index.html"));
  const html = await Bun.file("src/index.html").text();
  const injected = html.includes("</body>")
    ? html.replace("</body>", `${LIVE_RELOAD_SNIPPET}</body>`)
    : html + LIVE_RELOAD_SNIPPET;
  return new Response(injected, { headers: { "Content-Type": "text/html" } });
}

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
  .get("/", () => serveIndex())
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
  // Live-reload channel (DEV_HMR only): the browser connects here and reloads when the
  // BFF broadcasts "reload" after a source change.
  .ws("/__livereload", {
    open(ws) {
      liveReloadClients.add(ws as unknown as { send: (s: string) => void });
    },
    close(ws) {
      liveReloadClients.delete(ws as unknown as { send: (s: string) => void });
    },
  })
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
