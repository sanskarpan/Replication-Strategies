// Shared DOM + formatting helpers used across components. Kept dependency-free so it can
// be unit-tested under bun:test without a DOM (the pure functions) — the DOM helpers are
// thin wrappers over document.getElementById.

// prefers-reduced-motion, resolved once. Components gate animations on this.
export const reduceMotion =
  typeof window !== "undefined"
    ? (window.matchMedia?.("(prefers-reduced-motion: reduce)").matches ?? false)
    : false;

// esc HTML-escapes a string for safe innerHTML interpolation.
export function esc(s: string): string {
  return s.replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]!));
}

// decodeB64 decodes a Go-base64-encoded []byte value for display, tolerating non-base64.
export function decodeB64(v: string): string {
  try {
    return atob(v);
  } catch {
    return v;
  }
}

// byId is a typed getElementById that returns null (callers decide whether the element is
// required). Use req() when the element must exist.
export function byId<T extends HTMLElement = HTMLElement>(id: string): T | null {
  return document.getElementById(id) as T | null;
}

// req fetches a required element by id, throwing a clear error if the markup is missing.
export function req<T extends HTMLElement = HTMLElement>(id: string): T {
  const el = document.getElementById(id);
  if (!el) throw new Error(`required element #${id} not found`);
  return el as T;
}

// shortId renders the trailing segment of a node id (node-abc123-2 -> 2).
export function shortId(id: string): string {
  return id.split("-").slice(-1)[0];
}
