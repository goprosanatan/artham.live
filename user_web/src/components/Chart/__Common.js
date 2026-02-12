// Common helpers used by horizontal & vertical charts (non-breaking extraction)

export const hasWindow = typeof window !== "undefined";
export const getDPR = () => (hasWindow ? window.devicePixelRatio || 1 : 1);

export function clamp(v, a, b) {
  return Math.max(a, Math.min(b, v));
}
export function crisp(v) {
  return Math.round(v) + 0.5;
}
export function hexToRgb(hex) {
  const m = /^#?([\da-f]{2})([\da-f]{2})([\da-f]{2})$/i.exec(hex);
  if (!m) return [1, 1, 1];
  return [
    parseInt(m[1], 16) / 255,
    parseInt(m[2], 16) / 255,
    parseInt(m[3], 16) / 255,
  ];
}
export function normalizeWheel(e) {
  const LINE = 16;
  if (e.deltaMode === 1) return { x: e.deltaX * LINE, y: e.deltaY * LINE };
  if (e.deltaMode === 2) {
    const H = hasWindow ? window.innerHeight : 800;
    return { x: e.deltaX * H, y: e.deltaY * H };
  }
  return { x: e.deltaX, y: e.deltaY };
}
export function prepCanvas2D(canvas, w, h) {
  const dpr = getDPR();
  canvas.width = Math.floor(w * dpr);
  canvas.height = Math.floor(h * dpr);
  canvas.style.width = w + "px";
  canvas.style.height = h + "px";
  const ctx = canvas.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.imageSmoothingEnabled = false;
  return ctx;
}
export function roundRectPath(ctx, x, y, w, h, r) {
  const rr = Math.min(r, w / 2, h / 2);
  ctx.beginPath();
  ctx.moveTo(x + rr, y);
  ctx.arcTo(x + w, y, x + w, y + h, rr);
  ctx.arcTo(x + w, y + h, x, y + h, rr);
  ctx.arcTo(x, y + h, x, y, rr);
  ctx.arcTo(x, y, x + w, y, rr);
  ctx.closePath();
}

// Unified historical row mapper (expects bar_ts format)
export function mapHistoricalRow(row) {
  const r = typeof row === "string" ? JSON.parse(row) : row;
  let ts = r.bar_ts;
  const o = r.open ?? r.o ?? r.Open ?? r.O;
  const h = r.high ?? r.h ?? r.High ?? r.H;
  const l = r.low ?? r.l ?? r.Low ?? r.L;
  const c = r.close ?? r.c ?? r.Close ?? r.C ?? r.last_price ?? r.price;
  const v = r.volume ?? r.v ?? r.Volume ?? r.V ?? 0;
  return {
    date: ts,
    open: +o,
    high: +h,
    low: +l,
    close: +c,
    volume: +v || 0,
  };
}

// Tick step selection & formatting
export function choosePriceStep(range, timeframe = null) {
  // Use larger steps for 1D timeframe to increase label spacing
  const is1D = timeframe === "1D";
  const multiplier = is1D ? 2 : 1;
  
  if (range < 1) return 0.1 * multiplier;
  if (range < 2) return 0.2 * multiplier;
  if (range < 5) return 0.5 * multiplier;
  if (range < 15) return 1 * multiplier;
  if (range < 50) return 2 * multiplier;
  if (range < 200) return 5 * multiplier;
  if (range < 500) return 10 * multiplier;
  if (range < 2000) return 25 * multiplier;
  if (range < 5000) return 50 * multiplier;
  return 100 * multiplier;
}
export function decimalsForStep(step) {
  const s = Math.abs(step);
  const e = 1e-8;
  let d = 0,
    v = s;
  while (Math.abs(Math.round(v) - v) > e && d < 6) {
    v *= 10;
    d++;
  }
  return d;
}
export function formatPrice(v, dec = 2) {
  return Number.isFinite(v) ? v.toFixed(dec) : "-";
}
export function formatVol(v) {
  if (!Number.isFinite(v)) return "-";
  if (v >= 1e9) return (v / 1e9).toFixed(2) + "B";
  if (v >= 1e6) return (v / 1e6).toFixed(2) + "M";
  if (v >= 1e3) return (v / 1e3).toFixed(2) + "K";
  return String(v);
}

// Shared time label logic (accepts zoom metric)
export function formatTimeLabel(ts, timeframe, zoomMetric, is_intraday) {
  const d = new Date(ts);
  const zoomOut = zoomMetric < 8;
  const zoomIn = zoomMetric > 20;
  const intra =
    is_intraday || ["1m", "5m", "15m", "30m", "1h"].includes(timeframe);

  if (intra) {
    if (zoomOut)
      return d.toLocaleDateString("en-US", { month: "short", day: "2-digit" });
    if (zoomIn)
      return d.toLocaleTimeString("en-US", {
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
      });
    return d.toLocaleTimeString("en-US", {
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    });
  }
  if (timeframe === "1D") {
    if (zoomOut) {
      const m = d.toLocaleDateString("en-US", { month: "short" });
      const y = d.getFullYear();
      return d.getMonth() === 0 ? `${m} ${y}` : m;
    }
    return d.toLocaleDateString("en-US", { month: "short", day: "2-digit" });
  }
  if (["1W", "1M"].includes(timeframe)) {
    const m = d.toLocaleDateString("en-US", { month: "short" });
    const y = d.getFullYear();
    return zoomOut ? String(y) : `${m} ${y}`;
  }
  return d.toLocaleDateString("en-US", { month: "short", day: "2-digit" });
}
