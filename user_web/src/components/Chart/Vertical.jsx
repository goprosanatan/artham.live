// filepath: /Users/node/__Code/artham.live/api_web/src/components/Chart/vertical.jsx
import React, { useEffect, useMemo, useRef, useState } from "react";
import { useChartApi } from "@components/Chart/__API.js";
import { connectLive } from "@components/Chart/__Websocket.js";
import {
  clamp,
  crisp,
  hexToRgb,
  normalizeWheel,
  prepCanvas2D,
  roundRectPath,
  getDPR,
  mapHistoricalRow,
  choosePriceStep,
  decimalsForStep,
  formatPrice,
  formatVol,
  formatTimeLabel,
} from "@components/Chart/__Common.js";
import { ensureGL, bindInst } from "@components/Chart/__WebGL.js";
import { THEME_COLORS } from "@components/Chart/__Theme.js";
import { useAuth } from "@contexts/authProvider.jsx";
import { useThemeMode } from "flowbite-react";

export default function ChartVertical({
  instrumentId = null,
  className = "",
  showVolume = true,
  is_intraday = false,
}) {
  const {
    searchInstrument,
    getInstrumentDetail,
    getSegmentAll,
    getData,
  } = useChartApi();
  const { token } = useAuth();
  const { mode } = useThemeMode();
  const [candleData, setInternalData] = useState([]);
  const [selectedInstrumentId, setSelectedInstrumentId] =
    useState(instrumentId);
  const [selectedInstrumentDetail, setSelectedInstrumentDetail] =
    useState(null);
  const [slots, setSlots] = useState([]);
  const [timeframe, setTimeframe] = useState("1D");
  const [loadingCandles, setLoadingCandles] = useState(false);
  const [loadingMoreCandles, setLoadingMoreCandles] = useState(false);
  const [hasMoreData, setHasMoreData] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [segmentList, setSegmentList] = useState([]);
  const [wsConnected, setWsConnected] = useState(false);

  const socketRef = useRef(null);
  const loadingCandlesRef = useRef(loadingCandles);
  const timeframeRef = useRef(timeframe);
  const selectedInstrumentIdRef = useRef(selectedInstrumentId);

  useEffect(() => {
    loadingCandlesRef.current = loadingCandles;
  }, [loadingCandles]);

  useEffect(() => {
    timeframeRef.current = timeframe;
  }, [timeframe]);

  useEffect(() => {
    selectedInstrumentIdRef.current = selectedInstrumentId;
  }, [selectedInstrumentId]);

  async function loadCandlesForInstrument(
    instrumentId,
    timeframeOverride = null
  ) {
    if (!instrumentId) return;

    const resolvedTimeframe = timeframeOverride || timeframe;
    if (timeframeOverride && timeframeOverride !== timeframe) {
      setTimeframe(timeframeOverride);
    }

    // Reset hover/view state when switching instruments or timeframes
    setHoverIndex(null);
    setMouseXInner(null);
    velRef.current = 0;
    setOffsetY(0);
    setCandleHeight(18);

    // Clear previous dataset/slots to avoid flashing stale bars
    setInternalData([]);
    setSlots([]);

    setHasMoreData(true);
    setLoadingCandles(true);
    setLoadError("");
    try {
      const detail = await getInstrumentDetail(instrumentId).catch(() => null);
      if (detail && typeof detail === "object") {
        setSelectedInstrumentDetail(detail);
        console.log("Loaded instrument detail:", detail);
      } else {
        setSelectedInstrumentDetail(null);
      }

      const dataPayload = await getData({
        instrument_id: instrumentId,
        timeframe: resolvedTimeframe,
        timestamp_end: null,
      });

      const bars = Array.isArray(dataPayload?.bars)
        ? dataPayload.bars
        : [];

      const slots = Array.isArray(dataPayload?.slots)
        ? dataPayload.slots
        : [];

      setSlots(slots);

      const mapped = bars
        .map(mapHistoricalRow)
        .filter(
          (d) =>
            Number.isFinite(d.date) &&
            Number.isFinite(d.open) &&
            Number.isFinite(d.high) &&
            Number.isFinite(d.low) &&
            Number.isFinite(d.close)
        )
        .sort((a, b) => a.date - b.date);

      setInternalData(mapped);
      setSelectedInstrumentId(instrumentId);

      if (mapped.length < 300) {
        setHasMoreData(false);
      }
    } catch (e) {
      console.error("Historical load failed:", e);
      setLoadError("Failed to load historical data.");
      setInternalData([]);
      // setSelectedInstrumentDetail(null);
    } finally {
      setLoadingCandles(false);
    }
  }

  async function loadMoreCandles() {
    if (
      !selectedInstrumentId ||
      loadingMoreCandles ||
      !hasMoreData ||
      candleData.length === 0
    ) {
      return;
    }

    setLoadingMoreCandles(true);

    try {
      const finiteDates = candleData
        .map((d) => d?.date)
        .filter((d) => Number.isFinite(d));

      if (finiteDates.length === 0) {
        console.warn("loadMoreCandles: no finite dates found; stopping");
        setHasMoreData(false);
        return;
      }

      const earliestTimestamp = Math.min(...finiteDates);

      const historicalPayload = await getData({
        instrument_id: selectedInstrumentId,
        timeframe,
        timestamp_end: earliestTimestamp,
      });

      const historicalRows = Array.isArray(historicalPayload?.bars)
        ? historicalPayload.bars
        : [];

      const apiSlots = Array.isArray(historicalPayload?.slots)
        ? historicalPayload.slots
        : [];

      const mapped = historicalRows
        .map(mapHistoricalRow)
        .filter(
          (d) =>
            Number.isFinite(d.date) &&
            Number.isFinite(d.open) &&
            Number.isFinite(d.high) &&
            Number.isFinite(d.low) &&
            Number.isFinite(d.close) &&
            d.date < earliestTimestamp
        )
        .sort((a, b) => a.date - b.date);

      if (mapped.length === 0) {
        setHasMoreData(false);
      } else {
        setInternalData((prev) => [...mapped, ...prev]);
        setSlots((prev) => [...apiSlots, ...prev]);

        // Compensate scroll so the current viewport stays on the same bars after prepending
        const newSlotsCount = apiSlots.length || mapped.length;
        if (newSlotsCount > 0) {
          setOffsetY((prev) => prev - newSlotsCount * rowStride);
        }

        if (mapped.length < 300) {
          setHasMoreData(false);
        }
      }
    } catch (e) {
      console.error("Failed to load more candles:", e);
    } finally {
      setLoadingMoreCandles(false);
    }
  }

  useEffect(() => {
    async function fetchSegments() {
      const segments = await getSegmentAll();
      console.log("Fetched segments:", segments);
      setSegmentList(segments);
      console.log("Segment list loaded:", segmentList);
    }
    fetchSegments();

    if (instrumentId) loadCandlesForInstrument(instrumentId);
    else if (selectedInstrumentId)
      loadCandlesForInstrument(selectedInstrumentId);
  }, []);

  useEffect(() => {
    if (!instrumentId) return;
    const numericId = Number(instrumentId);
    if (!Number.isFinite(numericId)) return;
    if (numericId === selectedInstrumentId) return;
    loadCandlesForInstrument(numericId);
  }, [instrumentId]);

  useEffect(() => {
    if (!token) return;

    const { socket, disconnect, subscribe, unsubscribe } = connectLive({
      token,
      onConnect: () => setWsConnected(true),
      onDisconnect: () => setWsConnected(false),
      onAuthenticated: () => console.log("WS authenticated"),
      onUnauthorized: (data) => console.warn("WS unauthorized", data?.message),
      onBar: (payload) => {
        if (loadingCandlesRef.current) return;

        const { type, data: barData } = payload;

        let expectedType;
        if (timeframeRef.current === "1m") expectedType = "bars.1m";
        else if (timeframeRef.current === "1D") expectedType = "bars.1D";
        else {
          console.warn(`Unknown timeframe ${timeframeRef.current}, cannot process bar`);
          return;
        }

        if (type !== expectedType) return;
        if (
          String(barData.instrument_id) !==
          String(selectedInstrumentIdRef.current)
        )
          return;

        const bar = {
          date: barData.bar_ts,
          open: parseFloat(barData.open),
          high: parseFloat(barData.high),
          low: parseFloat(barData.low),
          close: parseFloat(barData.close),
          volume: parseInt(barData.volume, 10),
        };

        setInternalData((prev) => {
          if (!Array.isArray(prev) || prev.length === 0) return prev;
          const existingIndex = prev.findIndex((b) => b.date === bar.date);
          if (existingIndex !== -1) {
            const next = [...prev];
            next[existingIndex] = bar;
            return next;
          }
          return [...prev, bar];
        });
      },
      onError: (err) => console.error("WS error", err?.message || err),
    });

    socketRef.current = { socket, subscribe, unsubscribe, disconnect };

    return () => {
      socketRef.current = null;
      disconnect();
    };
  }, [token]);

  useEffect(() => {
    const ws = socketRef.current;
    if (!ws || !ws.subscribe || !ws.unsubscribe) return;
    if (!wsConnected || !selectedInstrumentId) return;

    let subType;
    if (timeframe === "1m") subType = "bars.1m";
    else if (timeframe === "1D") subType = "bars.1D";
    else {
      console.warn(`Unknown timeframe ${timeframe}, defaulting to bars.1m`);
      subType = "bars.1m";
    }

    ws.subscribe([selectedInstrumentId], subType);
    console.log("WS subscribe", selectedInstrumentId, subType);

    return () => {
      ws.unsubscribe([selectedInstrumentId], subType);
      console.log("WS unsubscribe", selectedInstrumentId, subType);
    };
  }, [wsConnected, selectedInstrumentId, timeframe]);

  const themeKey = mode === "dark" ? "dark" : "light";
  const colors = THEME_COLORS[themeKey] || THEME_COLORS.dark;
  const activeBg = colors.themeBg;
  const activeGrid = colors.gridColor;
  const activeText = colors.textColor;
  const crosshairColor = colors.crosshairColor;
  const chipBg = colors.chipBg;
  const chipBorder = colors.chipBorder;
  const chipText = colors.chipText;
  const panelBg = colors.panelBg;
  const panelBorder = colors.panelBorder;
  const controlBg = colors.controlBg;
  const controlBorder = colors.controlBorder;
  const controlText = colors.controlText;
  const activeControlBg = colors.activeBg;
  const activeControlBorder = colors.activeBorder;
  const activeControlText = colors.activeText;
  const emptyText = colors.emptyText;
  const loadingText = colors.loadingText;
  const searchPanelBg = colors.searchPanelBg;
  const searchInputBg = colors.searchInputBg;
  const badgeBg = colors.badgeBg;
  const buttonPrimaryBg = colors.buttonPrimaryBg;
  const buttonPrimaryBorder = colors.buttonPrimaryBorder;
  const buttonSecondaryBg = colors.buttonSecondaryBg;
  const buttonSecondaryBorder = colors.buttonSecondaryBorder;

  const [upCol, setUpCol] = useState(colors.upColor);
  const [downCol, setDownCol] = useState(colors.downColor);
  const [volEnabled, setVolEnabled] = useState(!!showVolume);
  const [wickScale, setWickScale] = useState(1.0);
  const [settingsOpen, setSettingsOpen] = useState(false);

  const [measuredHeight, setMeasuredHeight] = useState(480);
  const [width, setWidth] = useState(960);

  const containerRef = useRef(null);
  const chartGLRef = useRef(null);
  const chartLiveRef = useRef(null);
  const timeBaseRef = useRef(null);
  const timeLiveRef = useRef(null);
  const priceScaleBaseRef = useRef(null);
  const priceScaleLiveRef = useRef(null);

  const glRef = useRef(null);
  const glResRef = useRef(null);
  const vaoRef = useRef(null);
  const velRef = useRef(0);

  useEffect(() => {
    const el = containerRef.current;
    if (!el || typeof ResizeObserver === "undefined") return;
    const ro = new ResizeObserver((entries) => {
      setWidth(Math.max(320, Math.floor(entries[0].contentRect.width)));
      setMeasuredHeight(
        Math.max(200, Math.floor(entries[0].contentRect.height))
      );
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const LEFT = 90;
  const PRICE_SCALE_H = 28;
  const RIGHT = 10;
  const TOP = 8;
  const BOTTOM = 8;
  const BOTTOM_BAR_H = 60;
  const usableHeight = Math.max(40, measuredHeight - BOTTOM_BAR_H);
  const VOLUME_W = volEnabled ? 120 : 0; // volume overlay width (right side)
  const volumePadding = 8;
  const innerW = Math.max(40, width - LEFT - RIGHT);
  const candleW = Math.max(20, innerW); // candles span full width now
  const chartH = Math.max(40, usableHeight - PRICE_SCALE_H);

  const [candleHeight, setCandleHeight] = useState(18);
  const GAP = 4;
  const rowStride = candleHeight + GAP;
  const [offsetY, setOffsetY] = useState(0);
  const [mouse, setMouse] = useState({ x: null, y: null, inside: false });
  const [hoverIndex, setHoverIndex] = useState(null);
  const [mouseXInner, setMouseXInner] = useState(null);

  const [searchQuery, setSearchQuery] = useState("");
  const [searchOpen, setSearchOpen] = useState(false);
  const [searchResults, setSearchResults] = useState([]);
  const [searchLoading, setSearchLoading] = useState(false);
  const [searchExchange, setSearchExchange] = useState("");
  const [searchSegment, setSearchSegment] = useState("");

  const candlesRaw = useMemo(
    () =>
      (Array.isArray(candleData) ? candleData : [])
        .map((d) => ({
          date: new Date(d.date).getTime(),
          open: +d.open,
          high: +d.high,
          low: +d.low,
          close: +d.close,
          volume: d.volume ? +d.volume : 0,
        }))
        .sort((a, b) => a.date - b.date),
    [candleData]
  );

  const latestActualBar = useMemo(
    () => (candlesRaw.length ? candlesRaw[candlesRaw.length - 1] : null),
    [candlesRaw]
  );

  const candles = useMemo(() => {
    if (!slots.length) return candlesRaw;

    const baselineClose =
      candlesRaw.length && Number.isFinite(candlesRaw[candlesRaw.length - 1]?.close)
        ? candlesRaw[candlesRaw.length - 1].close
        : 0;

    const barMap = new Map();
    candlesRaw.forEach((bar) => {
      barMap.set(bar.date, bar);
    });

    return slots
      .map((slotTs) => {
        const ts = Number(slotTs);
        if (!Number.isFinite(ts)) return null;

        const bar = barMap.get(ts);
        if (bar) return bar;

        return {
          date: ts,
          open: baselineClose,
          high: baselineClose,
          low: baselineClose,
          close: baselineClose,
          volume: 0,
          isFuture: true,
        };
      })
      .filter(Boolean);
  }, [candlesRaw, slots]);

  const visibleRange = useMemo(() => {
    const n = candles.length;
    if (!n) return { start: 0, end: 0, yOff: 0 };
    const totalH = n * rowStride;
    const minOff = Math.min(0, chartH - TOP - BOTTOM - totalH);
    const yOff = clamp(offsetY, minOff - rowStride * 2, rowStride * 2);
    const dpr = getDPR(),
      PAD = 1 / dpr,
      bodyHalf = candleHeight / 2;
    const first = Math.floor((-yOff - (TOP + bodyHalf) + PAD) / rowStride) + 1;
    const last = Math.ceil(
      (chartH - BOTTOM - yOff + bodyHalf - PAD) / rowStride
    );
    const n1 = Math.max(0, n - 1);
    const start = clamp(first - 2, 0, n1);
    const end = clamp(last + 2, 0, n);

    if (
      end > n - 50 &&
      hasMoreData &&
      !loadingMoreCandles &&
      !loadingCandles
    ) {
      loadMoreCandles();
    }

    return { start, end, yOff };
  }, [
    candles.length,
    rowStride,
    chartH,
    offsetY,
    TOP,
    BOTTOM,
    candleHeight,
    hasMoreData,
    loadingMoreCandles,
    loadingCandles,
    loadMoreCandles,
  ]);

  const scale = useMemo(() => {
    if (!candles.length || visibleRange.end <= visibleRange.start)
      return { x: () => LEFT, min: 0, max: 1, range: 1 };
    let min = Infinity,
      max = -Infinity;
    for (let r = visibleRange.start; r < visibleRange.end; r++) {
      const i = candles.length - 1 - r;
      const b = candles[i];
      if (!b) continue;
      const priceSource = b.isFuture && latestActualBar ? latestActualBar : b;
      if (priceSource.low < min) min = priceSource.low;
      if (priceSource.high > max) max = priceSource.high;
    }
    if (min === max) {
      min -= 1;
      max += 1;
    }
    const pad = (max - min) * 0.05;
    min -= pad;
    max += pad;
    const range = max - min;
    const x = (p) => LEFT + ((p - min) * candleW) / range;
    return { min, max, range, x };
  }, [candles, visibleRange.start, visibleRange.end, candleW, LEFT]);

  const mappedCandles = useMemo(() => {
    if (!candles.length) return [];
    return candles.map((b) => {
      const priceSource = b.isFuture && latestActualBar ? latestActualBar : b;
      return {
        ...b,
        xOpen: scale.x(priceSource.open) - LEFT,
        xClose: scale.x(priceSource.close) - LEFT,
        xHigh: scale.x(priceSource.high) - LEFT,
        xLow: scale.x(priceSource.low) - LEFT,
      };
    });
  }, [candles, scale.min, scale.max, candleW, LEFT, latestActualBar]);

  useEffect(() => {
    const canvas = chartGLRef.current;
    if (!canvas) return;
    const gl = ensureGL(canvas, innerW, chartH, glRef, glResRef, vaoRef);
    if (!gl) return;

    const glr = glResRef.current;
    const vao = vaoRef.current;
    const n = mappedCandles.length;
    const { start, end } = visibleRange;

    const bgRGB = hexToRgb(activeBg);
    gl.clearColor(bgRGB[0], bgRGB[1], bgRGB[2], 1);
    gl.clear(gl.COLOR_BUFFER_BIT);
    if (!n || end <= start) return;

    gl.enable(gl.SCISSOR_TEST);
    const dpr = getDPR();
    const scW = Math.floor(innerW * dpr); // full width scissor
    const scH = Math.floor((chartH - TOP - BOTTOM) * dpr);
    const scY = Math.floor(chartH * dpr - TOP * dpr - scH);
    gl.scissor(0, scY, scW, scH);

    gl.useProgram(glr.program);
    gl.uniform2f(glr.uCanvas, innerW, chartH);

    const count = end - start;
    const maxCount = Math.min(count, glr.maxInstances);

    const bodyCenters = new Float32Array(maxCount * 2);
    const bodyHalf = new Float32Array(maxCount * 2);
    const bodyColors = new Float32Array(maxCount * 4);

    const wickCenters = new Float32Array(maxCount * 2);
    const wickHalf = new Float32Array(maxCount * 2);
    const wickColors = new Float32Array(maxCount * 4);

    const volCenters = new Float32Array(maxCount * 2);
    const volHalf = new Float32Array(maxCount * 2);
    const volColors = new Float32Array(maxCount * 4);

    const upRGB = hexToRgb(upCol);
    const downRGB = hexToRgb(downCol);
    const wickH = Math.max(1.6, getDPR() * wickScale);

    let bi = 0,
      wi = 0,
      vi = 0;
    const viewportTop = TOP - 1 / dpr;
    const viewportBot = chartH - BOTTOM + 1 / dpr;
    let maxVol = 0;
    if (volEnabled && VOLUME_W > 0) {
      for (let r = start; r < end; r++) {
        const i = n - 1 - r;
        const b = mappedCandles[i];
        if (b && b.volume > maxVol) maxVol = b.volume;
      }
      if (maxVol <= 0) maxVol = 1;
    }

    for (let r = start; r < end; r++) {
      const i = n - 1 - r;
      const b = mappedCandles[i];
      if (!b || b.isFuture) continue; // skip rendering placeholders
      const yC = TOP + visibleRange.yOff + r * rowStride + rowStride / 2;

      const wickUp = b.close >= b.open;
      const wickCol = wickUp ? upRGB : downRGB;
      const wTop = yC - wickH / 2;
      const wBot = yC + wickH / 2;
      if (wBot > viewportTop && wTop < viewportBot && wi < maxCount) {
        const cx = (b.xLow + b.xHigh) * 0.5;
        wickCenters[wi * 2] = cx;
        wickCenters[wi * 2 + 1] = yC;
        wickHalf[wi * 2] = Math.max(0.5, Math.abs(b.xHigh - b.xLow) * 0.5);
        wickHalf[wi * 2 + 1] = Math.max(0.5, wickH * 0.5);
        wickColors[wi * 4] = wickCol[0];
        wickColors[wi * 4 + 1] = wickCol[1];
        wickColors[wi * 4 + 2] = wickCol[2];
        wickColors[wi * 4 + 3] = 1.0;
        wi++;
      }

      const x1 = Math.min(b.xOpen, b.xClose);
      const x2 = Math.max(b.xOpen, b.xClose);
      const cx = (x1 + x2) * 0.5;
      const rgb = b.close >= b.open ? upRGB : downRGB;
      bodyCenters[bi * 2] = cx;
      bodyCenters[bi * 2 + 1] = yC;
      bodyHalf[bi * 2] = Math.max(0.5, (x2 - x1) * 0.5);
      bodyHalf[bi * 2 + 1] = Math.max(0.5, candleHeight * 0.5);
      bodyColors[bi * 4] = rgb[0];
      bodyColors[bi * 4 + 1] = rgb[1];
      bodyColors[bi * 4 + 2] = rgb[2];
      bodyColors[bi * 4 + 3] = 1.0;
      bi++;

      if (volEnabled && vi < maxCount && VOLUME_W > 0) {
        const ratio = Math.min(1, b.volume / maxVol);
        const barMaxW = Math.max(0, VOLUME_W - volumePadding - 6);
        const volW = Math.max(1, barMaxW * ratio);
        const rightEdge = innerW - volumePadding;
        const volCx = rightEdge - volW / 2;
        volCenters[vi * 2] = volCx;
        volCenters[vi * 2 + 1] = yC;
        volHalf[vi * 2] = volW / 2;
        volHalf[vi * 2 + 1] = Math.max(0.5, candleHeight * 0.5 * 0.8);
        volColors[vi * 4] = rgb[0];
        volColors[vi * 4 + 1] = rgb[1];
        volColors[vi * 4 + 2] = rgb[2];
        volColors[vi * 4 + 3] = 0.6;
        vi++;
      }
    }

    gl.bindVertexArray(vao);

    // Volume first (under candles)
    if (volEnabled && vi > 0) {
      gl.bindBuffer(gl.ARRAY_BUFFER, glr.instCenter);
      gl.bufferData(
        gl.ARRAY_BUFFER,
        volCenters.subarray(0, vi * 2),
        gl.DYNAMIC_DRAW
      );
      bindInst(gl, glr.instCenter, glr.iCenter, 2);
      gl.bindBuffer(gl.ARRAY_BUFFER, glr.instHalf);
      gl.bufferData(
        gl.ARRAY_BUFFER,
        volHalf.subarray(0, vi * 2),
        gl.DYNAMIC_DRAW
      );
      bindInst(gl, glr.instHalf, glr.iHalfSize, 2);
      gl.bindBuffer(gl.ARRAY_BUFFER, glr.instColor);
      gl.bufferData(
        gl.ARRAY_BUFFER,
        volColors.subarray(0, vi * 4),
        gl.DYNAMIC_DRAW
      );
      bindInst(gl, glr.instColor, glr.iColor, 4);
      gl.drawArraysInstanced(gl.TRIANGLES, 0, 6, vi);
    }

    if (wi > 0) {
      gl.bindBuffer(gl.ARRAY_BUFFER, glr.instCenter);
      gl.bufferData(
        gl.ARRAY_BUFFER,
        wickCenters.subarray(0, wi * 2),
        gl.DYNAMIC_DRAW
      );
      bindInst(gl, glr.instCenter, glr.iCenter, 2);
      gl.bindBuffer(gl.ARRAY_BUFFER, glr.instHalf);
      gl.bufferData(
        gl.ARRAY_BUFFER,
        wickHalf.subarray(0, wi * 2),
        gl.DYNAMIC_DRAW
      );
      bindInst(gl, glr.instHalf, glr.iHalfSize, 2);
      gl.bindBuffer(gl.ARRAY_BUFFER, glr.instColor);
      gl.bufferData(
        gl.ARRAY_BUFFER,
        wickColors.subarray(0, wi * 4),
        gl.DYNAMIC_DRAW
      );
      bindInst(gl, glr.instColor, glr.iColor, 4);
      gl.drawArraysInstanced(gl.TRIANGLES, 0, 6, wi);
    }

    if (bi > 0) {
      gl.bindBuffer(gl.ARRAY_BUFFER, glr.instCenter);
      gl.bufferData(
        gl.ARRAY_BUFFER,
        bodyCenters.subarray(0, bi * 2),
        gl.DYNAMIC_DRAW
      );
      bindInst(gl, glr.instCenter, glr.iCenter, 2);
      gl.bindBuffer(gl.ARRAY_BUFFER, glr.instHalf);
      gl.bufferData(
        gl.ARRAY_BUFFER,
        bodyHalf.subarray(0, bi * 2),
        gl.DYNAMIC_DRAW
      );
      bindInst(gl, glr.instHalf, glr.iHalfSize, 2);
      gl.bindBuffer(gl.ARRAY_BUFFER, glr.instColor);
      gl.bufferData(
        gl.ARRAY_BUFFER,
        bodyColors.subarray(0, bi * 4),
        gl.DYNAMIC_DRAW
      );
      bindInst(gl, glr.instColor, glr.iColor, 4);
      gl.drawArraysInstanced(gl.TRIANGLES, 0, 6, bi);
    }

    gl.bindVertexArray(null);
    gl.disable(gl.SCISSOR_TEST);
  }, [
    chartH,
    innerW,
    candleW,
    mappedCandles,
    visibleRange.start,
    visibleRange.end,
    visibleRange.yOff,
    candleHeight,
    rowStride,
    upCol,
    downCol,
    activeBg,
    TOP,
    BOTTOM,
    VOLUME_W,
    volumePadding,
    volEnabled,
    wickScale,
  ]);

  useEffect(() => {
    const c = priceScaleBaseRef.current;
    if (!c) return;
    const ctx = prepCanvas2D(c, innerW, PRICE_SCALE_H);
    ctx.fillStyle = activeBg;
    ctx.fillRect(0, 0, innerW, PRICE_SCALE_H);
    ctx.strokeStyle = activeGrid;
    ctx.lineWidth = 1;
    ctx.globalAlpha = 0.7;
    const ticks = 6;
    for (let i = 0; i <= ticks; i++) {
      const xx = crisp((i / ticks) * innerW);
      ctx.beginPath();
      ctx.moveTo(xx, 0);
      ctx.lineTo(xx, PRICE_SCALE_H);
      ctx.stroke();
    }
    const step = choosePriceStep(scale.range);
    const dec = decimalsForStep(step);
    ctx.globalAlpha = 1;
    ctx.fillStyle = activeText;
    ctx.font = "13px 'JetBrains Mono','Fira Code',monospace";
    ctx.textAlign = "center";
    ctx.textBaseline = "top";
    const PAD_X = 20;
    for (let i = 0; i <= ticks; i++) {
      const t = i / ticks;
      let value = scale.min + t * scale.range;
      value = Math.round(value / step) * step;
      value = Math.max(scale.min, Math.min(scale.max, value));
      let xx = t * innerW;
      xx = clamp(xx, PAD_X, innerW - PAD_X);
      ctx.fillText(formatPrice(value, dec), Math.floor(xx), 5);
    }
  }, [
    innerW,
    PRICE_SCALE_H,
    activeBg,
    activeGrid,
    activeText,
    scale.min,
    scale.max,
    scale.range,
  ]);

  useEffect(() => {
    const c = priceScaleLiveRef.current;
    if (!c) return;
    const ctx = prepCanvas2D(c, innerW, PRICE_SCALE_H);
    ctx.clearRect(0, 0, innerW, PRICE_SCALE_H);
    if (mouseXInner == null) return;
    const x = clamp(mouseXInner, 0, innerW);
    const price = scale.min + (x / innerW) * (scale.max - scale.min);
    const step = choosePriceStep(scale.range);
    const dec = Math.max(2, decimalsForStep(step) + 1);
    const label = formatPrice(price, dec);
    const pad = 6;
    ctx.font = "13px 'JetBrains Mono','Fira Code',monospace";
    const w = Math.max(86, Math.ceil(ctx.measureText(label).width) + pad * 2);
    const h = 20;
    const xx = clamp(x - w / 2, 2, innerW - w - 2);
    const y = 3;
    ctx.fillStyle = chipBg;
    ctx.strokeStyle = chipBorder;
    ctx.lineWidth = 1;
    roundRectPath(ctx, xx, y, w, h, 6);
    ctx.fill();
    ctx.stroke();
    ctx.fillStyle = chipText;
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";
    ctx.fillText(label, xx + w / 2, y + h / 2 + 0.5);
  }, [
    innerW,
    PRICE_SCALE_H,
    mouseXInner,
    scale.min,
    scale.max,
    scale.range,
    chipBg,
    chipBorder,
    chipText,
  ]);

  useEffect(() => {
    const c = timeBaseRef.current;
    if (!c) return;
    const ctx = prepCanvas2D(c, LEFT, usableHeight);
    ctx.fillStyle = activeBg;
    ctx.fillRect(0, 0, LEFT, usableHeight);
    ctx.fillStyle = activeText;
    ctx.font = "13px 'JetBrains Mono','Fira Code',monospace";
    ctx.textAlign = "right";
    ctx.textBaseline = "middle";
    const n = mappedCandles.length;
    let labelInterval;
    if (candleHeight < 10)
      labelInterval = Math.max(1, Math.floor(60 / rowStride));
    else if (candleHeight < 18)
      labelInterval = Math.max(1, Math.floor(50 / rowStride));
    else if (candleHeight < 30)
      labelInterval = Math.max(1, Math.floor(44 / rowStride));
    else labelInterval = Math.max(1, Math.floor(35 / rowStride));
    let prevY = -Infinity;
    const minGap = 40;
    for (let r = visibleRange.start; r < visibleRange.end; r += labelInterval) {
      const i = n - 1 - r;
      const y =
        PRICE_SCALE_H + TOP + visibleRange.yOff + r * rowStride + rowStride / 2;
      if (y < 0 || y > usableHeight) continue;
      if (y - prevY < minGap) continue;
      const label = formatTimeLabel(
        mappedCandles[i].date,
        timeframe,
        candleHeight,
        is_intraday
      );
      if (timeframe === "1D" && candleHeight < 12) {
        const d = new Date(mappedCandles[i].date);
        if (d.getMonth() === 0) {
          ctx.font = "bold 13px 'JetBrains Mono','Fira Code',monospace";
          ctx.fillText(label, LEFT - 6, Math.floor(y));
          ctx.font = "13px 'JetBrains Mono','Fira Code',monospace";
        } else ctx.fillText(label, LEFT - 6, Math.floor(y));
      } else ctx.fillText(label, LEFT - 6, Math.floor(y));
      prevY = y;
    }
  }, [
    LEFT,
    usableHeight,
    PRICE_SCALE_H,
    activeBg,
    activeText,
    mappedCandles,
    visibleRange.start,
    visibleRange.end,
    visibleRange.yOff,
    rowStride,
    candleHeight,
    timeframe,
    is_intraday,
    TOP,
  ]);

  useEffect(() => {
    const c = timeLiveRef.current;
    if (!c) return;
    const ctx = prepCanvas2D(c, LEFT, usableHeight);
    ctx.clearRect(0, 0, LEFT, usableHeight);
    if (hoverIndex == null || !mappedCandles.length) return;
    const n = mappedCandles.length;
    const r = n - 1 - hoverIndex;
    const y =
      PRICE_SCALE_H + TOP + visibleRange.yOff + r * rowStride + rowStride / 2;
    if (y < 0 || y > usableHeight) return;
    const d = new Date(mappedCandles[hoverIndex].date);
    const text =
      is_intraday || ["1m", "5m", "15m", "30m", "1h"].includes(timeframe)
        ? d.toLocaleDateString("en-US", { month: "short", day: "2-digit" }) +
          " " +
          d.toLocaleTimeString("en-US", {
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit",
            hour12: false,
          })
        : d.toLocaleDateString("en-US", {
            month: "short",
            day: "2-digit",
            year: "numeric",
          });
    const pad = 6;
    ctx.font = "13px 'JetBrains Mono','Fira Code',monospace";
    const w = Math.max(60, Math.ceil(ctx.measureText(text).width) + pad * 2);
    const h = 20;
    const x = 6;
    const yy = Math.floor(y - h / 2);
    ctx.fillStyle = chipBg;
    ctx.strokeStyle = chipBorder;
    ctx.lineWidth = 1;
    roundRectPath(ctx, x, yy, LEFT - 12, h, 6);
    ctx.fill();
    ctx.stroke();
    ctx.fillStyle = chipText;
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";
    ctx.fillText(text, x + (LEFT - 12) / 2, yy + h / 2 + 0.5);
  }, [
    LEFT,
    usableHeight,
    PRICE_SCALE_H,
    hoverIndex,
    mappedCandles,
    visibleRange.yOff,
    rowStride,
    timeframe,
    is_intraday,
    TOP,
    chipBg,
    chipBorder,
    chipText,
  ]);

  useEffect(() => {
    const c = chartLiveRef.current;
    if (!c) return;
    const ctx = prepCanvas2D(c, innerW, chartH);
    ctx.clearRect(0, 0, innerW, chartH);
    if (hoverIndex == null || !mappedCandles.length) return;
    const n = mappedCandles.length;
    const idx = clamp(hoverIndex, 0, n - 1);
    const r = n - 1 - idx;
    const yC = TOP + visibleRange.yOff + r * rowStride + rowStride / 2;
    if (yC < TOP || yC > chartH - BOTTOM) return;
    if (mouseXInner != null) {
      const xLine = clamp(mouseXInner, 0, innerW);
      ctx.strokeStyle = crosshairColor;
      ctx.setLineDash([4, 4]);
      ctx.beginPath();
      ctx.moveTo(xLine, 0);
      ctx.lineTo(xLine, chartH);
      ctx.stroke();
      ctx.setLineDash([]);
    }
    ctx.strokeStyle = crosshairColor;
    ctx.setLineDash([4, 4]);
    ctx.beginPath();
    ctx.moveTo(0, Math.round(yC));
    ctx.lineTo(innerW, Math.round(yC));
    ctx.stroke();
    ctx.setLineDash([]);
    const b = mappedCandles[idx];
    const priceSource = b?.isFuture && latestActualBar ? latestActualBar : b;
    const isUp = priceSource.close >= priceSource.open;
    const color = isUp ? upCol : downCol;
    const pct =
      priceSource.open !== 0
        ? ((priceSource.close - priceSource.open) / priceSource.open) * 100
        : 0;
    const text = `O:${formatPrice(priceSource.open)}  H:${formatPrice(
      priceSource.high
    )}  L:${formatPrice(priceSource.low)}  C:${formatPrice(priceSource.close)}  Δ ${
      pct >= 0 ? "+" : ""
    }${pct.toFixed(2)}%  V:${formatVol(b.volume)}`;
    const pad = 8;
    ctx.font = "14px 'JetBrains Mono','Fira Code',monospace";
    const textW = Math.ceil(ctx.measureText(text).width);
    const panelW = textW + pad * 2,
      panelH = 24,
      panelX = 8,
      panelY = chartH - panelH - 8;
    ctx.fillStyle = panelBg;
    ctx.strokeStyle = panelBorder;
    ctx.lineWidth = 1;
    roundRectPath(ctx, panelX, panelY, panelW, panelH, 6);
    ctx.fill();
    ctx.stroke();
    ctx.fillStyle = color;
    ctx.textAlign = "left";
    ctx.textBaseline = "middle";
    ctx.fillText(text, panelX + pad, panelY + panelH / 2 + 0.5);
  }, [
    innerW,
    chartH,
    hoverIndex,
    mappedCandles,
    visibleRange.yOff,
    rowStride,
    mouseXInner,
    TOP,
    BOTTOM,
    upCol,
    downCol,
    crosshairColor,
    panelBg,
    panelBorder,
  ]);

  useEffect(() => {
    let raf = 0;
    const tick = () => {
      if (Math.abs(velRef.current) > 0.1) {
        setOffsetY((o) => {
          const minOff = Math.min(
            0,
            chartH - TOP - BOTTOM - mappedCandles.length * rowStride
          );
          return clamp(
            o + velRef.current,
            minOff - rowStride * 2,
            rowStride * 2
          );
        });
        velRef.current *= 0.88;
        raf = requestAnimationFrame(tick);
      }
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [mappedCandles.length, rowStride, chartH, TOP, BOTTOM]);

  useEffect(() => {
    const target = containerRef.current;
    if (!target) return;
    let dragging = false,
      lastY = 0;
    const recomputeHover = (y) => {
      const n = mappedCandles.length;
      if (!n) return null;
      if (y >= PRICE_SCALE_H && y < PRICE_SCALE_H + chartH) {
        const localY = y - PRICE_SCALE_H;
        const r = Math.floor((localY - (TOP + visibleRange.yOff)) / rowStride);
        const i = n - 1 - r;
        return clamp(i, 0, n - 1);
      }
      return null;
    };
    const onEnter = () => setMouse((m) => ({ ...m, inside: true }));
    const onLeave = () => {
      setMouse({ x: null, y: null, inside: false });
      setHoverIndex(null);
      setMouseXInner(null);
      velRef.current = 0;
    };
    const onMove = (e) => {
      if (searchOpen) return;
      const r = target.getBoundingClientRect();
      const x = e.clientX - r.left;
      const y = e.clientY - r.top;
      setMouse({ x, y, inside: true });
      const xi = x - LEFT;
      if (xi >= 0 && xi <= innerW && y >= 0 && y <= PRICE_SCALE_H + chartH)
        setMouseXInner(xi);
      else setMouseXInner(null);
      setHoverIndex(recomputeHover(y));
    };
    const onDown = (e) => {
      if (searchOpen) return;
      try {
        target.setPointerCapture?.(e.pointerId ?? 0);
      } catch {}
      dragging = true;
      lastY = e.clientY;
      velRef.current = 0;
    };
    const onUp = () => {
      dragging = false;
    };
    const onDrag = (e) => {
      if (searchOpen || !dragging) return;
      const dy = e.clientY - lastY;
      lastY = e.clientY;
      velRef.current = dy;
      setOffsetY((o) => {
        const minOff = Math.min(
          0,
          chartH - TOP - BOTTOM - mappedCandles.length * rowStride
        );
        return clamp(o + dy, minOff - rowStride * 2, rowStride * 2);
      });
      setHoverIndex(mouse.inside ? recomputeHover(mouse.y) : null);
    };
    const onWheel = (e) => {
      if (searchOpen) return;
      e.preventDefault();
      const { x: dX, y: dY } = normalizeWheel(e);
      const r = target.getBoundingClientRect();
      const x = e.clientX - r.left;
      const y = e.clientY - r.top;
      const xi = x - LEFT;
      if (xi >= 0 && xi <= innerW) setMouseXInner(xi);
      else setMouseXInner(null);
      if (Math.abs(dX) > Math.abs(dY) || e.ctrlKey) {
        const delta = e.ctrlKey ? dY : dX;
        const factor = Math.exp(-delta * 0.002);
        const newBar = clamp(candleHeight * factor, 6, 56);
        const prev = rowStride;
        const next = newBar + GAP;
        const localY = y - PRICE_SCALE_H;
        setOffsetY((oy) => {
          const rIdx = (localY - (TOP + oy + prev / 2)) / prev;
          const nextOff = localY - (rIdx * next + next / 2) - TOP;
          const minOff = Math.min(
            0,
            chartH - TOP - BOTTOM - mappedCandles.length * next
          );
          return clamp(nextOff, minOff - next * 2, next * 2);
        });
        setCandleHeight(newBar);
        velRef.current = 0;
      } else {
        setOffsetY((o) => {
          const minOff = Math.min(
            0,
            chartH - TOP - BOTTOM - mappedCandles.length * rowStride
          );
          const next = o - dY * 0.7;
          return clamp(next, minOff - rowStride * 2, rowStride * 2);
        });
        velRef.current = -dY * 0.7;
      }
      setHoverIndex(recomputeHover(y));
    };
    target.addEventListener("mouseenter", onEnter);
    target.addEventListener("mouseleave", onLeave);
    target.addEventListener("pointermove", onMove);
    target.addEventListener("pointerdown", onDown);
    window.addEventListener("pointerup", onUp);
    window.addEventListener("pointermove", onDrag);
    target.addEventListener("wheel", onWheel, { passive: false });
    return () => {
      target.removeEventListener("mouseenter", onEnter);
      target.removeEventListener("mouseleave", onLeave);
      target.removeEventListener("pointermove", onMove);
      target.removeEventListener("pointerdown", onDown);
      window.removeEventListener("pointerup", onUp);
      window.removeEventListener("pointermove", onDrag);
      target.removeEventListener("wheel", onWheel);
    };
  }, [
    candleHeight,
    rowStride,
    visibleRange.yOff,
    GAP,
    LEFT,
    innerW,
    PRICE_SCALE_H,
    chartH,
    mappedCandles.length,
    mouse.inside,
    mouse.y,
    TOP,
    BOTTOM,
    searchOpen,
  ]);

  useEffect(() => {
    if (!searchQuery.trim()) {
      setSearchLoading(false);
      setSearchResults([]);
      return;
    }
    const trimmed = searchQuery.trim();
    if (trimmed.length < 3) {
      setSearchLoading(false);
      setSearchResults([]);
      return;
    }
    const handle = setTimeout(async () => {
      setSearchLoading(true);
      try {
        const res = await searchInstrument(
          searchExchange,
          searchSegment,
          trimmed
        );
        setSearchResults(Array.isArray(res) ? res : []);
      } catch (err) {
        console.error("search error:", err);
        setSearchResults([]);
      } finally {
        setSearchLoading(false);
      }
    }, 250);
    return () => clearTimeout(handle);
  }, [searchQuery, searchExchange, searchSegment]);

  const ChartArea = (
    <>
      <div
        style={{
          position: "absolute",
          left: 0,
          top: 0,
          width: LEFT,
          height: usableHeight,
        }}
      >
        <canvas ref={timeBaseRef} style={{ position: "absolute", inset: 0 }} />
        <canvas
          ref={timeLiveRef}
          className="pointer-events-none"
          style={{ position: "absolute", inset: 0 }}
        />
      </div>
      <div
        style={{
          position: "absolute",
          left: LEFT,
          top: 0,
          width: innerW,
          height: PRICE_SCALE_H,
        }}
      >
        <canvas
          ref={priceScaleBaseRef}
          style={{ position: "absolute", inset: 0 }}
        />
        <canvas
          ref={priceScaleLiveRef}
          className="pointer-events-none"
          style={{ position: "absolute", inset: 0 }}
        />
      </div>
      <div
        style={{
          position: "absolute",
          left: LEFT,
          top: PRICE_SCALE_H,
          width: innerW,
          height: chartH,
        }}
      >
        <canvas ref={chartGLRef} style={{ position: "absolute", inset: 0 }} />
        <canvas
          ref={chartLiveRef}
          className="pointer-events-none"
          style={{ position: "absolute", inset: 0 }}
        />
      </div>
      <div
        style={{
          position: "absolute",
          right: 0,
          top: 0,
          width: RIGHT,
          height: usableHeight,
        }}
      />
      {!mappedCandles.length && !loadingCandles && (
        <div
          className="absolute inset-0 flex items-center justify-center text-sm"
          style={{ color: emptyText }}
        >
          {selectedInstrumentId
            ? "No candles for selected symbol."
            : "Search and select a symbol to render."}
        </div>
      )}
      {loadingCandles && (
        <div
          className="absolute inset-0 flex items-center justify-center text-sm"
          style={{ color: loadingText }}
        >
          Loading {timeframe} candles...
        </div>
      )}
      {!!loadError && (
        <div className="absolute inset-x-0 top-2 flex items-center justify-center">
          <div className="px-3 py-1.5 rounded bg-red-500/10 text-red-300 border border-red-500/40 text-xs">
            {loadError}
          </div>
        </div>
      )}
    </>
  );

  return (
    <div
      ref={containerRef}
      className={`relative w-full ${className}`}
      style={{ background: activeBg, overflow: "hidden" }}
    >
      <div
        className={searchOpen ? "blur-[10px] pointer-events-none" : ""}
        style={{ position: "absolute", inset: 0, paddingBottom: BOTTOM_BAR_H }}
      >
        {ChartArea}
      </div>
      <div
        className="absolute left-0 bottom-0 w-full flex items-center gap-2"
        style={{
          height: BOTTOM_BAR_H,
          zIndex: 5,
          padding: "0 10px",
          backgroundColor: chipBg,
          borderTop: `1px solid ${chipBorder}`,
        }}
      >
        <button
          type="button"
          onClick={() => setSettingsOpen(true)}
          title="Chart Settings"
          className="border rounded p-1 text-base leading-none cursor-pointer hover:opacity-80 transition-colors"
          style={{
            backgroundColor: controlBg,
            borderColor: controlBorder,
            color: controlText,
          }}
        >
          <span style={{ fontSize: 16, lineHeight: 1 }}>⚙️</span>
        </button>
        <input
          readOnly
          placeholder={
            selectedInstrumentDetail
              ? `${selectedInstrumentDetail.segment} : ${selectedInstrumentDetail.trading_symbol}`
              : "Search instruments..."
          }
          onFocus={() => setSearchOpen(true)}
          className="flex-1 text-sm px-3 py-2 rounded border outline-none focus:border-blue-500 transition-colors"
          style={{
            backgroundColor: controlBg,
            borderColor: controlBorder,
            color: controlText,
          }}
        />
        {["1m", "5m", "15m", "1h", "1D"].map((t) => (
          <button
            key={t}
            type="button"
            onClick={async () => {
              if (t !== timeframe) setTimeframe(t);
              if (selectedInstrumentId)
                await loadCandlesForInstrument(selectedInstrumentId, t);
            }}
            className="px-2.5 py-1.5 text-xs rounded border transition-colors cursor-pointer hover:opacity-80"
            style={{
              backgroundColor:
                timeframe === t ? activeControlBg : controlBg,
              borderColor:
                timeframe === t ? activeControlBorder : controlBorder,
              color: timeframe === t ? activeControlText : controlText,
            }}
          >
            {t}
          </button>
        ))}
      </div>

      {searchOpen && (
        <div
          className="absolute inset-0 flex items-start justify-center"
          style={{ zIndex: 30, paddingTop: 80 }}
          onClick={() => setSearchOpen(false)}
        >
          <div
            className="pointer-events-auto rounded-md p-3 shadow-lg w-3/4 border"
            style={{
              backgroundColor: searchPanelBg,
              borderColor: panelBorder,
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center mb-3 gap-3">
              <input
                autoFocus
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                placeholder="Search instruments..."
                className="flex-1 text-sm px-2 py-1 rounded border outline-none focus:border-blue-500 transition-colors"
                style={{
                  backgroundColor: searchInputBg,
                  borderColor: controlBorder,
                  color: controlText,
                }}
              />
              <select
                value={searchSegment}
                onChange={(e) => setSearchSegment(e.target.value)}
                className="text-xs px-2 py-1 rounded border outline-none transition-colors"
                style={{
                  backgroundColor: searchInputBg,
                  borderColor: controlBorder,
                  color: controlText,
                }}
              >
                <option value="">ALL</option>
                {segmentList.map((segment) => (
                  <option key={segment} value={segment}>
                    {segment}
                  </option>
                ))}
              </select>
            </div>
            {searchLoading && (
              <div
                className="py-8 text-center text-sm"
                style={{ color: loadingText }}
              >
                Searching...
              </div>
            )}
            {!searchLoading && searchResults.length === 0 && (
              <div
                className="py-8 text-center text-sm"
                style={{ color: emptyText }}
              >
                No results
              </div>
            )}
            {!searchLoading && searchResults.length > 0 && (
              <div className="max-h-[360px] overflow-y-auto">
                {searchResults.map((instrument) => {
                  return (
                    <button
                      key={instrument.instrument_id}
                      className="w-full flex items-center justify-between px-3 py-2 transition-colors text-left"
                      style={{
                        color: controlText,
                        backgroundColor: "transparent",
                      }}
                      onClick={async () => {
                        setSearchOpen(false);
                        setSearchQuery("");
                        await loadCandlesForInstrument(
                          instrument.instrument_id
                        );
                      }}
                    >
                      <div className="flex flex-col text-left">
                        <span
                          className="text-sm font-medium tracking-tight"
                          style={{ color: activeControlText }}
                        >
                          {instrument.description}
                        </span>
                        <span
                          className="text-[11px]"
                          style={{ color: emptyText }}
                        >
                          {`${instrument.exchange} — ${instrument.trading_symbol}`}
                        </span>
                      </div>
                      <div className="flex items-center gap-2 text-[11px]">
                        <span
                          className="px-2 py-0.5 rounded"
                          style={{ backgroundColor: badgeBg, color: controlText }}
                        >
                          {instrument.segment}
                        </span>
                      </div>
                    </button>
                  );
                })}
              </div>
            )}
          </div>
        </div>
      )}

      {settingsOpen && (
        <div
          className="absolute inset-0 flex items-center justify-center"
          style={{ background: "rgba(0,0,0,0.45)", zIndex: 20 }}
          onClick={(e) => {
            if (e.target === e.currentTarget) setSettingsOpen(false);
          }}
        >
          <div
            className="rounded-lg shadow-xl w-[380px] p-4 border"
            style={{ backgroundColor: colors.modalBg, borderColor: colors.modalBorder }}
          >
            <div
              className="text-base font-semibold mb-3"
              style={{ color: activeControlText }}
            >
              Chart Settings
            </div>
            <div
              className="space-y-3 text-sm"
              style={{ color: controlText }}
            >
              <Setting label="Candle Height">
                <input
                  type="range"
                  min={6}
                  max={56}
                  step={1}
                  value={candleHeight}
                  onChange={(e) =>
                    setCandleHeight(parseInt(e.target.value, 10))
                  }
                  className="w-full"
                />
                <span className="w-10 text-right font-mono">
                  {candleHeight}
                </span>
              </Setting>
              <Setting label="Wick Thickness">
                <input
                  type="range"
                  min={0.6}
                  max={1.8}
                  step={0.1}
                  value={wickScale}
                  onChange={(e) => setWickScale(parseFloat(e.target.value))}
                  className="w-full"
                />
                <span className="w-10 text-right font-mono">
                  {wickScale.toFixed(1)}x
                </span>
              </Setting>
              <Setting label="Up Color">
                <input
                  type="color"
                  value={upCol}
                  onChange={(e) => setUpCol(e.target.value)}
                />
                <span className="font-mono">{upCol}</span>
              </Setting>
              <Setting label="Down Color">
                <input
                  type="color"
                  value={downCol}
                  onChange={(e) => setDownCol(e.target.value)}
                />
                <span className="font-mono">{downCol}</span>
              </Setting>
              <Setting label="Show Volume">
                <input
                  type="checkbox"
                  checked={volEnabled}
                  onChange={() => setVolEnabled((v) => !v)}
                />
              </Setting>
              <div className="flex items-center justify-end gap-2 pt-2">
                <button
                  type="button"
                  onClick={() => {
                    const palette = THEME_COLORS[themeKey] || THEME_COLORS.dark;
                    setUpCol(palette.upColor);
                    setDownCol(palette.downColor);
                    setVolEnabled(!!showVolume);
                    setWickScale(1.0);
                    setCandleHeight(18);
                  }}
                  className="px-3 py-1.5 text-sm rounded border transition-colors"
                  style={{
                    backgroundColor: buttonSecondaryBg,
                    borderColor: buttonSecondaryBorder,
                    color: activeControlText,
                  }}
                >
                  Reset
                </button>
                <button
                  type="button"
                  onClick={() => setSettingsOpen(false)}
                  className="px-3 py-1.5 text-sm rounded border transition-colors"
                  style={{
                    backgroundColor: buttonPrimaryBg,
                    borderColor: buttonPrimaryBorder,
                    color: colors.priceChipText,
                  }}
                >
                  Close
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function Setting({ label, children }) {
  return (
    <div className="flex items-center justify-between gap-3">
      <label className="w-40">{label}</label>
      <div className="flex items-center gap-2 flex-1">{children}</div>
    </div>
  );
}
