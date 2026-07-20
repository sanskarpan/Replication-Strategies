import { byId, esc } from "./dom";

export type ToastKind = "info" | "error" | "success" | "warn";

// toast surfaces a non-blocking notification (replaces alert()). It auto-dismisses.
export function toast(message: string, kind: ToastKind = "info", title?: string): void {
  const container = byId("toast-container");
  if (!container) {
    console.warn(message);
    return;
  }
  const el = document.createElement("div");
  el.className = `toast ${kind === "info" ? "" : kind}`.trim();
  el.innerHTML = title ? `<div class="toast-title">${esc(title)}</div>${esc(message)}` : esc(message);
  container.appendChild(el);
  setTimeout(() => {
    el.style.opacity = "0";
    el.style.transition = "opacity 0.3s";
    setTimeout(() => el.remove(), 300);
  }, 4000);
}

// reportError surfaces an operation error as an error toast.
export function reportError(context: string, e: unknown): void {
  const msg = e instanceof Error ? e.message : String(e);
  toast(msg, "error", context);
}
