import type { VectorClock } from "../api/types";
import { esc, shortId } from "./dom";

// displayResult pretty-prints an API result, decoding base64 "value" fields for display
// (Go JSON-encodes []byte as base64).
export function displayResult(result: unknown): string {
  return JSON.stringify(
    result,
    (key, val) => {
      if (key === "value" && typeof val === "string") {
        try {
          return atob(val);
        } catch {
          return val;
        }
      }
      return val;
    },
    2,
  );
}

// vcChipsHTML renders a vector clock as compact per-node chips instead of raw JSON.
export function vcChipsHTML(vc: VectorClock | undefined): string {
  const entries = Object.entries(vc || {}).filter(([, n]) => n > 0);
  if (entries.length === 0) return `<span class="vc-chips empty">∅</span>`;
  return (
    `<span class="vc-chips">` +
    entries
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([id, n]) => `<span class="vc-chip"><b>${esc(shortId(id))}</b><span>:${n}</span></span>`)
      .join("") +
    `</span>`
  );
}

// avgLatency formats the mean of a latency sample array. Empty -> "—"; a sub-1ms mean
// (including all-zeros) -> "<1ms" (matches the original metric-card behavior).
export function avgLatency(samples: number[]): string {
  if (!samples || samples.length === 0) return "—";
  const avg = samples.reduce((a, b) => a + b, 0) / samples.length;
  return avg < 1 ? "<1ms" : `${avg.toFixed(1)}ms`;
}

// pctl returns the p-th percentile (nearest-rank) of a latency sample array, formatted
// with the same "—"/"<1ms" convention as avgLatency.
export function pctl(samples: number[], p: number): string {
  if (!samples || samples.length === 0) return "—";
  const v = pctlValue(samples, p);
  return v < 1 ? "<1ms" : `${v.toFixed(1)}ms`;
}

// pctlValue is the numeric nearest-rank percentile (0 for empty input).
export function pctlValue(samples: number[], p: number): number {
  if (!samples || samples.length === 0) return 0;
  const s = [...samples].sort((a, b) => a - b);
  const rank = Math.max(0, Math.ceil((p / 100) * s.length) - 1);
  return s[Math.min(rank, s.length - 1)];
}

// fmtChartMs formats a latency-chart bar value: 0 -> "—", sub-1ms -> "<1ms", else "Xms".
export function fmtChartMs(v: number): string {
  return v === 0 ? "—" : v < 1 ? "<1ms" : `${v.toFixed(1)}ms`;
}
