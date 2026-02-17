import { useEffect, useMemo, useRef, useState, useCallback } from "react";
import { useChartApi, useOrderApi } from "@components/Chart/__API.js";
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


// ============================================================
// Viewport Engine (centralized time/price viewport abstraction)
// ============================================================
class ViewportEngine {
  constructor({ initialBarWidth, initialOffsetX }) {
    this.time = {
      barWidth: initialBarWidth,
      offsetX: initialOffsetX,
      anchor: "right",
    };
    this.price = {
      mode: "auto",
      min: null,
      max: null,
    };
  }

  setBarWidth(bw) {
    this.time.barWidth = bw;
  }

  setOffsetX(ox) {
    this.time.offsetX = ox;
    this.time.anchor = "free";
  }

  setPriceRange(min, max) {
    this.price.min = min;
    this.price.max = max;
    this.price.mode = "manual";
  }

  resetPriceRange() {
    this.price.min = null;
    this.price.max = null;
    this.price.mode = "auto";
  }
}
// ============================================================


// Static layout and rendering constants
const TIME_SCALE_H = 62; // Height of time axis at bottom (pixels)
const PRICE_SCALE_W = 42; // Width of price axis at right (pixels)
const BOTTOM_BAR_H = 60; // Height of control bar with timeframe buttons and search
const TOP = 0; // Top padding inside chart area (pixels)
const BOTTOM = 8; // Bottom padding inside chart area (pixels)
const LEFT = 8; // Left padding inside chart area (pixels)
const RIGHT = 8; // Right padding inside chart area (pixels)
const SHOW_BODY_THRESHOLD = 3; // Minimum body width in pixels to render bar bodies

// Zoom-dependent inter-bar gap computation (in pixels)
const computeGap = (w) => {
  if (w >= 72) return 8; // Very zoomed in
  if (w >= 48) return 5; // Zoomed in
  if (w >= 24) return 4; // Medium zoom
  if (w >= 12) return 3; // Default zoom
  if (w >= 6) return 2; // Zoomed out
  return 1; // Very zoomed out
};

// Validates selector levels to ensure target/stop sit on opposite sides of entry.
// Returns direction (long/short), readiness flag, and an error message when invalid.
const validateSelectorLevels = (levels) => {
  const entry = levels.entry?.price;
  const target = levels.target?.price;
  const stop = levels.stop?.price;

  if (entry == null)
    return { direction: null, ready: false, error: "", invalidKey: null };

  if (target == null)
    return { direction: null, ready: false, error: "", invalidKey: null };

  if (target === entry) {
    return {
      direction: null,
      ready: false,
      error: "Target must be above or below the entry to set direction.",
      invalidKey: "target",
    };
  }

  const direction = target > entry ? "long" : "short";

  if (stop == null) return { direction, ready: false, error: "", invalidKey: null };

  const stopValid = direction === "long" ? stop < entry : stop > entry;
  if (!stopValid) {
    return {
      direction,
      ready: false,
      error:
        direction === "long"
          ? "For long trades, target above entry and stop below entry."
          : "For short trades, target below entry and stop above entry.",
      invalidKey: "stop",
    };
  }

  return { direction, ready: true, error: "", invalidKey: null };
};

const MONTHS = [
  "Jan",
  "Feb",
  "Mar",
  "Apr",
  "May",
  "Jun",
  "Jul",
  "Aug",
  "Sep",
  "Oct",
  "Nov",
  "Dec",
];

const formatMonthLabel = (date) =>
  date.getMonth() === 0 ? String(date.getFullYear()) : MONTHS[date.getMonth()];

// Snap price to nearest valid tick multiple
const snapToTickSize = (price, tickSize) => {
  if (!tickSize || tickSize <= 0) return price;
  const ticks = Math.round(price / tickSize);
  return ticks * tickSize;
};

// Main horizontal chart component handling data loading, rendering, and UI controls.
export default function ChartVertical({
  instrumentId = null,
  className = "",
  showVolume = true,
  is_intraday = false,
  externalOrder = null,
  onClearOrder = null,
  onOrderSubmitted = null,
}) {
  const isVertical = true;
  // ViewportEngine instance (persisted across renders)
  const viewportRef = useRef(null);
  if (!viewportRef.current) {
    viewportRef.current = new ViewportEngine({
      initialBarWidth: 12,
      initialOffsetX: 0,
    });
  }
  const viewport = viewportRef.current;
  // REST helpers used to bootstrap instruments and historical bars.
  const {
    searchInstrument,
    getInstrumentDetail,
    getSegmentAll,
    getData,
  } = useChartApi();

  const { submitBracket } = useOrderApi();

  // Local state for loaded bars and selected instrument metadata.
  const [barData, setBarData] = useState([]);
  const [selectedInstrumentId, setSelectedInstrumentId] =
    useState(instrumentId);
  const [selectedInstrumentDetail, setSelectedInstrumentDetail] =
    useState(null);
  const [slots, setSlots] = useState([]);

  const [timeframe, setTimeframe] = useState("1m");
  const [loadingBars, setLoadingBars] = useState(false);
  const [loadingMoreBars, setLoadingMoreBars] = useState(false);
  const [hasMoreData, setHasMoreData] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [segmentList, setSegmentList] = useState([]);
  const [selectorActive, setSelectorActive] = useState(false); // false = crosshair, true = trade selector
  const [selectorLevels, setSelectorLevels] = useState({
    entry: { price: null, startIndex: null, endIndex: null },
    target: { price: null, startIndex: null, endIndex: null },
    stop: { price: null, startIndex: null, endIndex: null },
  });
  const [selectorStep, setSelectorStep] = useState("entry"); // entry -> stop -> target
  const [submittingOrder, setSubmittingOrder] = useState(false);
  const [submitToast, setSubmitToast] = useState({ message: "", tone: "success" });
  const [orderQty, setOrderQty] = useState(1);
  const crosshairEnabled = !selectorActive;
  const selectorDragRef = useRef(null); // Active drag target: entry | target | stop
  const lastPannedOrderIdRef = useRef(null); // Track which order we auto-panned to
  // State to trigger WebGL gridline update after label Xs are set
  const [labelGridVersion, setLabelGridVersion] = useState(0);

  // Acquire auth token for authenticated websocket feeds.
  const { token } = useAuth();
  const { mode, setMode } = useThemeMode();

  // Ref holding websocket connection object with subscribe/unsubscribe/disconnect methods
  const socketRef = useRef(null);

  // Tracks whether websocket is connected and authenticated (enables subscription effect)
  const [wsConnected, setWsConnected] = useState(false);

  // Refs to track current values for websocket callbacks.
  // WebSocket event handlers capture state at connection time, causing stale closure issues.
    // Ref to share time label anchor Xs between time label effect and WebGL gridline effect
    const labelAnchorXsRef = useRef([]);
  // These refs are updated immediately when state changes, so WS callbacks always see current values.
  const loadingBarsRef = useRef(loadingBars); // Prevents processing bars during initial load
  const timeframeRef = useRef(timeframe); // Filters incoming bars by timeframe
  const selectedInstrumentIdRef = useRef(selectedInstrumentId); // Filters bars by instrument

  // Sync loadingBars state to ref whenever it changes
  // Websocket onBar handler checks this to skip processing during initial data load
  useEffect(() => {
    loadingBarsRef.current = loadingBars;
  }, [loadingBars]);

  // Keep hover state even when selector mode is active

  // Reset selector levels whenever mode toggles
  useEffect(() => {
    setSelectorLevels({
      entry: { price: null, startIndex: null, endIndex: null },
      target: { price: null, startIndex: null, endIndex: null },
      stop: { price: null, startIndex: null, endIndex: null },
    });
    setSelectorStep("entry");
  }, [selectorActive]);

  // Sync timeframe state to ref whenever it changes
  // Websocket onBar handler uses this to filter bars by type (bars.1m vs bars.1D)
  useEffect(() => {
    timeframeRef.current = timeframe;
  }, [timeframe]);

  // Reset and disable trade selector when timeframe or instrument changes
  useEffect(() => {
    setSelectorActive(false);
    setSelectorLevels({
      entry: { price: null, startIndex: null, endIndex: null },
      target: { price: null, startIndex: null, endIndex: null },
      stop: { price: null, startIndex: null, endIndex: null },
    });
    setSelectorStep("entry");
    setSelectorError("");
    selectorDragRef.current = null;
  }, [timeframe, selectedInstrumentId]);

  // Sync selectedInstrumentId state to ref whenever it changes
  // Websocket onBar handler uses this to ignore bars for other instruments
  useEffect(() => {
    selectedInstrumentIdRef.current = selectedInstrumentId;
  }, [selectedInstrumentId]);

  /**
   * Requests historical bars and resets view for the given instrument/timeframe.
   * This is the primary data loading function called when user selects an instrument.
   *
   * @param {number} instrumentId - Numeric ID of the instrument to load
   * @param {string|null} timeframeOverride - Optional timeframe ("1m" or "1D"), defaults to current timeframe
   */
  async function loadBarsForInstrument(instrumentId, timeframeOverride = null) {
    // Guard: exit early if no instrument ID provided
    if (!instrumentId) return;

    // Determine which timeframe to use: override parameter or current state
    const resolvedTimeframe = timeframeOverride || timeframe;

    // If timeframe override provided and differs from current state, update it
    if (timeframeOverride && timeframeOverride !== timeframe) {
      setTimeframe(timeframeOverride); // Update state (triggers ref sync and WS resubscription)
      console.log("Timeframe changed to:", timeframeOverride);
    }

    // Clear hover/crosshair UI when switching instruments (prevents stale hover state)
    setHoverIndex(null); // Remove bar index under cursor
    setMouseYInner(null); // Remove Y position for price crosshair

    // Reset view (pan/zoom/inertia) so a new instrument starts from the default right-aligned view
    velRef.current = 0; // Stop any ongoing inertia from the previous instrument
    prevBarLengthRef.current = 0; // Allow auto-scroll to work for the new dataset
    setBarWidth(12); // Reset zoom level
    setOffsetX(0); // Temporary neutral pan; recenter effect will right-align after data loads

    // Clear previous dataset to avoid showing stale bars while the new instrument loads
    setBarData([]);
    setSlots([]);

    // Set loading flag (shows "Loading Bars..." UI and blocks live bar processing)
    setLoadingBars(true);

    // Clear any previous error messages from failed loads
    setLoadError("");

    // Assume more historical data is available until proven otherwise
    setHasMoreData(true);

    try {
      // Fetch instrument metadata (trading symbol, segment, exchange, etc.) for display
      // Use .catch(() => null) to prevent throwing on fetch failure
      const instrumentDetail = await getInstrumentDetail(instrumentId).catch(
        () => null
      );

      // Validate and store instrument detail if successfully fetched
      if (instrumentDetail && typeof instrumentDetail === "object") {
        setSelectedInstrumentDetail(instrumentDetail); // Used for control bar display
        console.log("Loaded instrument detail:", instrumentDetail);
      } else {
        // Clear detail if fetch failed
        setSelectedInstrumentDetail(null);
      }

      // Fetch historical OHLCV bars from API
      const historicalPayload = await getData({
        instrument_id: instrumentId, // Which instrument to fetch
        timeframe: resolvedTimeframe, // Which timeframe (1m, 1D, etc.)
        timestamp_end: null, // null = fetch up to current time (most recent bars)
      });

      const historicalRows = Array.isArray(historicalPayload?.bars)
        ? historicalPayload.bars
        : [];

      const apiSlots = Array.isArray(historicalPayload?.slots)
        ? historicalPayload.slots
        : [];

      console.log("Loaded slots:", apiSlots.length);
      setSlots(apiSlots);

      // Transform API response format to internal format and validate data integrity
      const mappedBarsList = historicalRows
        .map(mapHistoricalRow) // Convert API row format to {date, open, high, low, close, volume}
        .filter(
          (bar) =>
            // Filter out invalid bars with NaN or Infinity values
            // Prevents rendering errors and scale calculation issues
            Number.isFinite(bar.date) &&
            Number.isFinite(bar.open) &&
            Number.isFinite(bar.high) &&
            Number.isFinite(bar.low) &&
            Number.isFinite(bar.close)
        )
        .sort((a, b) => a.date - b.date); // Sort chronologically (oldest → newest)

      // Update bar data state (triggers chart re-render)
      setBarData(mappedBarsList);

      // Update selected instrument ID (triggers websocket resubscription)
      setSelectedInstrumentId(instrumentId);

      // If we received fewer than 300 bars, we've hit the end of historical data
      if (mappedBarsList.length < 300) {
        setHasMoreData(false); // Disables lazy loading trigger in visibleRange
      }
    } catch (e) {
      // Handle any network errors, API errors, or parsing errors
      console.error("Historical load failed:", e);
      setLoadError("Failed to load historical data."); // Display error message to user
      setBarData([]); // Clear bar data (shows empty state)
      // Note: keeping instrument detail even on error (commented line)
      // setSelectedInstrumentDetail(null);
    } finally {
      // Always clear loading flag when done (success or failure)
      // This re-enables live bar processing in websocket handler
      setLoadingBars(false);
    }
  }

  /**
   * Fetches older bars and prepends them while preserving the viewport.
   * Called automatically by visibleRange memo when user scrolls within first 50 bars.
   * Implements infinite scroll / lazy loading for historical data.
   */
  async function loadMoreBars() {
    // Guard conditions: skip if any of these prevent loading more data
    if (
      !selectedInstrumentId || // No instrument selected
      loadingMoreBars || // Already loading more data (prevents duplicate requests)
      !hasMoreData || // No more historical data available from API
      barData.length === 0
    ) {
      // No existing data to extend
      return;
    }

    // Set loading flag (displays "Loading older data..." badge in UI)
    setLoadingMoreBars(true);

    try {
      // Find the timestamp of the oldest currently loaded bar
      // Guard against any malformed bars to avoid NaN leaking into requests
      const finiteDates = barData
        .map((historicalBar) => historicalBar?.date)
        .filter((d) => Number.isFinite(d));

      if (finiteDates.length === 0) {
        console.warn(
          "loadMoreBars: no finite dates found; stopping pagination"
        );
        setHasMoreData(false);
        return;
      }

      const earliestTimestamp = Math.min(...finiteDates);

      // Log pagination request for debugging
      console.log("Loading more bars before:", new Date(earliestTimestamp));

      // Fetch 300 bars older than the earliest currently loaded bar
      const historicalPayload = await getData({
        instrument_id: selectedInstrumentId, // Current instrument
        timeframe: timeframe, // Current timeframe
        timestamp_end: earliestTimestamp, // Fetch bars before this timestamp
      });

      // Defensive: ensure response is array
      const historicalRows = Array.isArray(historicalPayload?.bars)
        ? historicalPayload.bars
        : [];

      const apiSlots = Array.isArray(historicalPayload?.slots)
        ? historicalPayload.slots
        : [];

      // Transform and validate bars, filtering out any overlap with existing data
      const mappedHistoricalBars = historicalRows
        .map(mapHistoricalRow) // Convert API format to internal format
        .filter(
          (bar) =>
            // Validate numeric fields are finite (not NaN or Infinity)
            Number.isFinite(bar.date) &&
            Number.isFinite(bar.open) &&
            Number.isFinite(bar.high) &&
            Number.isFinite(bar.low) &&
            Number.isFinite(bar.close) &&
            bar.date < earliestTimestamp // CRITICAL: only bars strictly before earliest (no overlap)
        )
        .sort((a, b) => a.date - b.date); // Sort chronologically (oldest → newest)

      // Check if we've reached the end of historical data
      if (mappedHistoricalBars.length === 0) {
        setHasMoreData(false); // Disable future lazy loading
        console.log("No more historical data available");
      } else {
        // Count how many new slots we're adding (needed for viewport compensation)
        const newSlotsCount = apiSlots.length;

        // Prepend new bars to the beginning of existing data
        // This shifts all existing bar indices to the right by newBarsCount
        setBarData((previousBars) => [
          ...mappedHistoricalBars, // New older bars at the beginning
          ...previousBars, // Existing bars after
        ]);

        // Prepend new slots to the beginning
        setSlots((previousSlots) => [...apiSlots, ...previousSlots]);

        // CRITICAL: Adjust viewport offset to maintain visual position
        // When we prepend slots, all indices shift right by newSlotsCount
        // So we shift viewport left by (newSlotsCount * colStride) pixels to compensate
        // This keeps the user looking at the same bars before and after load
        setOffsetX(
          (previousOffset) => previousOffset - newSlotsCount * colStride
        );

        // Log success for debugging
        console.log(`Loaded ${mappedHistoricalBars.length} more bars`);

        // If fewer than 300 bars returned, we're near the end of historical data
        if (mappedHistoricalBars.length < 300) {
          setHasMoreData(false); // Disable future lazy loading
        }
      }
    } catch (e) {
      // Log any fetch or parsing errors (non-fatal, doesn't affect existing data)
      console.error("Failed to load more bars:", e);
    } finally {
      // Always clear loading flag when done (success or failure)
      setLoadingMoreBars(false);
    }
  }

  // On mount, only fetch segment list (not bars)
  useEffect(() => {
    async function fetchSegments() {
      const segments = await getSegmentAll();
      setSegmentList(segments);
    }
    fetchSegments();
  }, []);

  // Reload when parent passes a new instrumentId
  useEffect(() => {
    if (!instrumentId) return;
    const numericId = Number(instrumentId);
    if (!Number.isFinite(numericId)) return;
    if (numericId === selectedInstrumentId) return;
    loadBarsForInstrument(numericId);
  }, [instrumentId]);

  // Websocket lifecycle: establish connection and handle real-time bar updates
  useEffect(() => {
    // Guard: skip websocket setup if no auth token available
    if (!token) return;

    // Initialize websocket connection with event handlers
    // connectLive returns control methods: socket, disconnect, subscribe, unsubscribe
    const { socket, disconnect, subscribe, unsubscribe } = connectLive({
      token, // Auth token for websocket authentication

      // Called when websocket connection is successfully established
      onConnect: () => {
        setWsConnected(true); // Update state (triggers subscription effect)
      },

      // Called when websocket connection is lost (network issue, server restart, etc.)
      onDisconnect: () => {
        setWsConnected(false); // Update state (subscription effect will unsubscribe)
      },

      // Called when websocket authentication succeeds
      onAuthenticated: () => {
        console.log("WS authenticated");
      },

      // Called when websocket authentication fails (invalid token, expired, etc.)
      onUnauthorized: (data) => {
        console.warn("WS unauthorized", data?.message);
      },

      // Called when a real-time bar update is received from server
      // Handles both in-progress bar updates (same minute getting new ticks) and new bars (time rollover)
      onBar: (payload) => {
        // CRITICAL: Skip processing if historical data is still loading
        // Prevents race condition where live bars arrive before initial load completes
        // This would cause index mismatches and incorrect bar placement
        if (loadingBarsRef.current) return;

        // Destructure payload: {type: "bars.1m" | "bars.1D", data: {bar fields}}
        const { type, data: barData } = payload;

        // // Debug log for monitoring live data flow
        // console.log("📊 Bar received:", {
        //   type, // Bar type (bars.1m, bars.1D)
        //   instrument_id: barData.instrument_id, // Which instrument
        //   bar_ts: barData.bar_ts, // Bar timestamp
        //   close: barData.close, // Current close price
        // });

        // Determine expected bar type based on current timeframe
        // We only process bars matching the timeframe user is viewing
        let expectedType;
        if (timeframeRef.current === "1m") {
          expectedType = "bars.1m"; // Expecting 1-minute bars
        } else if (timeframeRef.current === "1D") {
          expectedType = "bars.1D"; // Expecting daily bars
        } else {
          // Unknown/unsupported timeframe, can't process bar
          console.warn(
            `Unknown timeframe ${timeframeRef.current}, cannot process bar`
          );
          return;
        }

        // Filter: skip bar if type doesn't match current timeframe
        if (type !== expectedType) return;

        // Filter: skip bar if it's for a different instrument than currently displayed
        // Convert both to strings for comparison (handle numeric vs string IDs)
        if (
          String(barData.instrument_id) !==
          String(selectedInstrumentIdRef.current)
        )
          return;

        // Parse and validate bar data from websocket payload
        const bar = {
          date: barData.bar_ts, // Bar timestamp (milliseconds since epoch)
          open: parseFloat(barData.open), // Opening price
          high: parseFloat(barData.high), // Highest price in period
          low: parseFloat(barData.low), // Lowest price in period
          close: parseFloat(barData.close), // Closing/current price
          volume: parseInt(barData.volume, 10), // Trading volume
        };

        // Update bar data with new/updated bar
        setBarData((previousBars) => {
          // Guard: skip update if no existing data (shouldn't happen, but be defensive)
          if (!Array.isArray(previousBars) || previousBars.length === 0)
            return previousBars;

          // Check if bar with same timestamp already exists
          // Timestamps match when it's an update to the current in-progress bar
          const existingIndex = previousBars.findIndex(
            (existingBar) => existingBar.date === bar.date
          );

          if (existingIndex !== -1) {
            // Bar exists: UPDATE it (in-progress bar receiving new ticks)
            // Example: 9:30 AM bar gets updated as new trades come in during 9:30-9:31
            const updatedBars = [...previousBars]; // Clone array
            updatedBars[existingIndex] = bar; // Replace existing bar with updated data
            return updatedBars;
          } else {
            // Bar doesn't exist: APPEND it (new bar from time rollover)
            // Example: clock hit 9:31 AM, so we get a new 9:31 bar
            return [...previousBars, bar];
          }
        });
      },

      // Called when websocket encounters an error (network, protocol, etc.)
      onError: (err) => {
        console.error("WS error", err?.message || err);
      },
    });

    // Store websocket control methods in ref for subscription effect to access
    socketRef.current = { socket, subscribe, unsubscribe, disconnect };

    // Cleanup function: runs when component unmounts or token changes
    return () => {
      socketRef.current = null; // Clear ref
      disconnect(); // Close websocket connection cleanly
    };
  }, [token]); // Dependency: only reconnect when auth token changes

  // Manage websocket subscriptions: subscribe/unsubscribe when connection state or instrument/timeframe changes
  useEffect(() => {
    // Get websocket control methods from ref (set by websocket lifecycle effect)
    const ws = socketRef.current;

    // Guard: skip if websocket not initialized or methods not available
    if (!ws || !ws.subscribe || !ws.unsubscribe) return;

    // Guard: skip if not connected or no instrument selected
    if (!wsConnected || !selectedInstrumentId) return;

    // Determine which bar subscription type to use based on current timeframe
    let subType;
    if (timeframe === "1m") {
      subType = "bars.1m"; // Subscribe to 1-minute bar updates
    } else if (timeframe === "1D") {
      subType = "bars.1D"; // Subscribe to daily bar updates
    } else {
      // Unknown timeframe, default to 1-minute with warning
      console.warn(`Unknown timeframe ${timeframe}, defaulting to bars.1m`);
      subType = "bars.1m";
    }

    // Subscribe to bar updates for selected instrument
    // Server will start sending real-time bar updates via onBar handler
    ws.subscribe([selectedInstrumentId], subType);
    console.log("WS subscribe", selectedInstrumentId, subType);

    // Cleanup function: unsubscribe when instrument/timeframe changes or component unmounts
    // Prevents receiving bars for instruments/timeframes user is no longer viewing
    return () => {
      ws.unsubscribe([selectedInstrumentId], subType);
      console.log("WS unsubscribe", selectedInstrumentId, subType);
    };
  }, [wsConnected, selectedInstrumentId, timeframe]); // Re-run when connection state, instrument, or timeframe changes

  // Chart appearance controls exposed via the settings panel.
  // These settings can be modified by the user through the settings modal.
  const [volEnabled, setVolEnabled] = useState(!!showVolume); // Toggle volume bars on/off
  const [wickScale, setWickScale] = useState(1.0); // Wick line thickness multiplier (0.6-1.8x)
  const [settingsOpen, setSettingsOpen] = useState(false); // Settings modal visibility

  // Use Flowbite theme as the single source of truth
  const themeKey = mode === "dark" ? "dark" : "light";

  // Extract current theme's color palette based on Flowbite mode
  const colors = THEME_COLORS[themeKey]; // Get dark or light theme object
  const activeBg = colors.themeBg; // Background color for canvases
  const activeGrid = colors.gridColor; // Grid line color
  const activeText = colors.textColor; // Text label color

  // User-customizable bar colors (initialized from theme, can be changed in settings)
  // Separate state allows users to override theme defaults with custom colors
  const [upCol, setUpCol] = useState(colors.upColor); // Color for bullish bars (close >= open)
  const [downCol, setDownCol] = useState(colors.downColor); // Color for bearish bars (close < open)

  // Container dimensions tracked via ResizeObserver (responsive to parent container)
  const [measuredHeight, setMeasuredHeight] = useState(480); // Total container height in pixels
  const [width, setWidth] = useState(960); // Total container width in pixels

  // References for layered canvases and GL resources.
  // Chart uses a multi-layer canvas architecture: static layers + interactive overlays.
  const containerRef = useRef(null); // Main container div (for ResizeObserver)
  const chartBaseRef = useRef(null); // WebGL canvas for bars/wicks/volume (GPU-accelerated)
  const chartOverlayRef = useRef(null); // 2D overlay for crosshair and hover tooltip
  const timeBaseRef = useRef(null); // 2D canvas for time scale grid and labels
  const timeOverlayRef = useRef(null); // 2D overlay for hovered time chip
  const priceBaseRef = useRef(null); // 2D canvas for price scale grid and labels
  const priceOverlayRef = useRef(null); // 2D overlay for hover/live price chips

  // WebGL context and resource refs (preserved across re-renders)
  const glRef = useRef(null); // WebGL2 rendering context
  const glResRef = useRef(null); // WebGL resources (shaders, buffers, uniforms)
  const vaoRef = useRef(null); // Vertex Array Object for instanced rendering
  const velRef = useRef(0); // Pan velocity for inertia animation (pixels per frame)

  // Track container size changes and update canvas dimensions accordingly
  // Uses ResizeObserver API for efficient, throttled resize detection
  useEffect(() => {
    const el = containerRef.current; // Get container element

    // Guard: skip if element not mounted or ResizeObserver not supported
    if (!el || typeof ResizeObserver === "undefined") return;

    // Create observer that fires when container dimensions change
    const ro = new ResizeObserver((entries) => {
      // Update width state with minimum 320px (prevents unusable narrow charts)
      setWidth(Math.max(320, Math.floor(entries[0].contentRect.width)));

      // Update height state with minimum 200px (prevents unusable short charts)
      setMeasuredHeight(
        Math.max(200, Math.floor(entries[0].contentRect.height))
      );
    });

    // Start observing container element
    ro.observe(el);

    // Cleanup: disconnect observer when component unmounts
    return () => ro.disconnect();
  }, []); // Empty deps = run once on mount

  // Layout constants for canvas regions and padding.
  // These define the size of UI elements and internal padding for the chart.
  const VOLUME_H = volEnabled ? 160 : 0; // Height of volume section (0 when disabled)

  // Compute chart, volume, and GL canvas dimensions from container size.
  // Vertical orientation: time axis on left, price axis on top.
  const usableHeight = Math.max(40, measuredHeight - BOTTOM_BAR_H); // Space above control bar
  const verticalTimelineHeight = Math.max(40, usableHeight - PRICE_SCALE_W);
  const verticalPriceWidth = Math.max(40, width - TIME_SCALE_H);
  const chartAreaH = Math.max(40, verticalPriceWidth - VOLUME_H); // Bars-only height (before rotation)
  const glHeight = chartAreaH + VOLUME_H; // Combined height for chart + volume
  const chartAreaW = verticalTimelineHeight; // Rotated chart span

  // Bar width in pixels (user-adjustable via settings, range: 1-100)
  const [barWidth, setBarWidthState] = useState(viewport.time.barWidth); // Default 12px width
  // Keep viewport in sync with barWidth
  const setBarWidth = (bw) => {
    viewport.setBarWidth(bw);
    setBarWidthState(bw);
  };

  // Determines spacing between bars for different zoom levels (module-scope computeGap)

  // Memoized gap value based on current bar width (recomputes when width changes)
  const gap = useMemo(() => computeGap(barWidth), [barWidth]);

  // Total horizontal space occupied by one bar (body + gap)
  const colStride = barWidth + gap;

  // Minimum width threshold for rendering bar bodies (module-scope SHOW_BODY_THRESHOLD)

  // Track horizontal pan offset and cursor hover information.
  // offsetX represents how many pixels the chart is shifted left/right from origin.
  const [offsetX, setOffsetXState] = useState(viewport.time.offsetX);
  // Keep viewport in sync with offsetX
  const setOffsetX = (ox) => {
    viewport.setOffsetX(ox);
    setOffsetXState(ox);
  };

  // Mouse cursor position and hover state
  const [mouse, setMouse] = useState({ x: null, y: null, inside: false }); // Cursor position relative to container
  const [hoverIndex, setHoverIndex] = useState(null); // Index of bar under cursor (null if none)
  const [mouseYInner, setMouseYInner] = useState(null); // Y position within chart area (for price crosshair)
  const [selectorError, setSelectorError] = useState("");
  const mouseRef = useRef(mouse);
  useEffect(() => {
    mouseRef.current = mouse;
  }, [mouse]);
  const selectorValidation = useMemo(
    () => validateSelectorLevels(selectorLevels),
    [selectorLevels]
  );
  const selectorReady = selectorValidation.ready;

  const round2 = (value) =>
    value == null || Number.isNaN(value)
      ? null
      : Math.round(Number(value) * 100) / 100;

  const drawAxisLabelText = (ctx, text, x, y, rotationDeg = 90) => {
    ctx.save();
    ctx.translate(x, y);
    ctx.rotate((rotationDeg * Math.PI) / 180);
    ctx.fillText(text, 0, 0);
    ctx.restore();
  };

  // Get tick size from instrument detail (default 0.05 if not available)
  const tickSize = useMemo(() => {
    const ts = selectedInstrumentDetail?.tick_size;
    return ts && Number.isFinite(Number(ts)) && Number(ts) > 0 ? Number(ts) : 0.05;
  }, [selectedInstrumentDetail]);

  const resolveLevelRange = (level) => {
    if (!timelineBars.length) {
      return {
        startIndex: null,
        endIndex: null,
        startTs: null,
        endTs: null,
      };
    }

    const rawStartIndex = Number.isFinite(level?.startIndex) ? level.startIndex : null;
    const rawEndIndex = Number.isFinite(level?.endIndex) ? level.endIndex : null;
    const maxIndex = timelineBars.length - 1;

    const startIndex = rawStartIndex == null ? null : clamp(rawStartIndex, 0, maxIndex);
    const endIndex = rawEndIndex == null ? null : clamp(rawEndIndex, 0, maxIndex);

    const startTs = startIndex == null ? null : timelineBars[startIndex]?.date ?? null;
    const endTs = endIndex == null ? null : timelineBars[endIndex]?.date ?? null;

    return { startIndex, endIndex, startTs, endTs };
  };

  // Submit bracket order to backend OMS
  const handleEnterTrade = async () => {
    if (!selectorReady || !selectedInstrumentDetail || !selectedInstrumentId)
      return;

    // Validate order quantity
    const qtyValue = String(orderQty).trim();
    if (!qtyValue) {
      setSelectorError("Please enter a quantity");
      return;
    }

    const qty = Number(qtyValue);
    if (!Number.isFinite(qty)) {
      setSelectorError("Quantity must be a valid number");
      return;
    }

    if (qty <= 0) {
      setSelectorError("Quantity must be greater than 0");
      return;
    }

    if (qty > 1000000) {
      setSelectorError("Quantity cannot exceed 1,000,000");
      return;
    }

    if (!Number.isInteger(qty)) {
      setSelectorError("Quantity must be a whole number");
      return;
    }

    const direction = selectorValidation.direction;
    const transaction_type = direction === "long" ? "BUY" : "SELL";

    // Snap prices to tick size multiples
    const snappedEntry = snapToTickSize(selectorLevels.entry?.price, tickSize);
    const snappedTarget = snapToTickSize(selectorLevels.target?.price, tickSize);
    const snappedStop = snapToTickSize(selectorLevels.stop?.price, tickSize);

    const entryRange = resolveLevelRange({
      ...selectorLevels.entry,
      endIndex: lastActualIndex,
    });
    const targetRange = resolveLevelRange({
      ...selectorLevels.target,
      endIndex: lastActualIndex,
    });
    const stopRange = resolveLevelRange({
      ...selectorLevels.stop,
      endIndex: lastActualIndex,
    });

    const payload = {
      strategy_id: "manual_ui",
      instrument_id: Number(selectedInstrumentId),
      side: transaction_type,
      qty: qty,
      entry_price: round2(snappedEntry),
      target_price: round2(snappedTarget),
      stoploss_price: round2(snappedStop),
      entry_start_ts: entryRange.startTs,
      entry_end_ts: entryRange.endTs,
      target_start_ts: targetRange.startTs,
      target_end_ts: targetRange.endTs,
      stop_start_ts: stopRange.startTs,
      stop_end_ts: stopRange.endTs,
      symbol: selectedInstrumentDetail.trading_symbol,
      exchange: selectedInstrumentDetail.exchange,
    };

    setSubmittingOrder(true);
    setSelectorError("");
    try {
      await submitBracket(payload);
      // Clear selector UI while still submitting
      setSelectorLevels({
        entry: { price: null, startIndex: null, endIndex: null },
        target: { price: null, startIndex: null, endIndex: null },
        stop: { price: null, startIndex: null, endIndex: null },
      });
      setSelectorStep("entry");
      setSelectorActive(false);
      
      // Wait for order list to update via callback before showing success
      if (onOrderSubmitted) {
        try {
          await onOrderSubmitted();
        } catch (callbackErr) {
          console.error("onOrderSubmitted callback failed", callbackErr);
          throw callbackErr; // Treat callback failure as order failure
        }
      }
      
      // Only after order list is updated, reset button and show success toast
      setSubmittingOrder(false);
      setSubmitToast({ message: "Trade submitted", tone: "success" });
    } catch (err) {
      console.error("submitBracket failed", err);
      setSubmittingOrder(false);
      setSubmitToast({ message: "Failed to submit order. Please retry.", tone: "error" });
    }
  };

  // Auto-hide submit toast after a short delay
  useEffect(() => {
    if (!submitToast.message) return;
    const timer = setTimeout(() => setSubmitToast({ message: "", tone: submitToast.tone }), 2500);
    return () => clearTimeout(timer);
  }, [submitToast]);

  // Manage instrument search panel visibility and filters.
  // Search panel appears as modal overlay when user clicks search input.
  const [searchOpen, setSearchOpen] = useState(false); // Search modal visibility
  const [searchResults, setSearchResults] = useState([]); // Array of matching instruments
  const [searchLoading, setSearchLoading] = useState(false); // Loading state during API search

  // Search filter criteria (used to construct API query)
  const [searchQuery, setSearchQuery] = useState(""); // Text query (symbol name, company name)
  const [searchExchange, setSearchExchange] = useState(""); // Exchange filter (NSE, BSE, etc.)
  const [searchSegment, setSearchSegment] = useState(""); // Segment filter (EQ, FO, etc.)

  // Normalizes raw bar data for rendering and ensures chronological order.
  // Transforms API response format into standardized internal format with validated numeric types.
  const bars = useMemo(() => {
    // Guard: return empty array if barData is not an array
    if (!Array.isArray(barData)) return [];

    return barData
      .map((d) => ({
        date: new Date(d.date).getTime(), // Convert date to milliseconds timestamp (ensures numeric)
        open: +d.open, // Coerce to number (handles string prices from API)
        high: +d.high, // Coerce to number
        low: +d.low, // Coerce to number
        close: +d.close, // Coerce to number
        volume: d.volume ? +d.volume : 0, // Coerce to number with fallback to 0
      }))
      .sort((a, b) => a.date - b.date); // Sort chronologically (oldest → newest)
  }, [barData]); // Recompute only when raw barData changes

  // Latest actual bar (excludes placeholders) for live price / baseline math
  const latestActualBar = useMemo(
    () => (bars.length ? bars[bars.length - 1] : null),
    [bars]
  );

  // Build timeline from slots, placing bars at their slot positions
  const timelineBars = useMemo(() => {
    if (!slots.length) return bars; // Fallback if no slots

    // Get baseline close for placeholders
    const baselineClose =
      bars.length && Number.isFinite(bars[bars.length - 1]?.close)
        ? bars[bars.length - 1].close
        : 0;

    // Create a map of bar timestamps to bar data for O(1) lookup
    const barMap = new Map();
    bars.forEach((bar) => {
      barMap.set(bar.date, bar);
    });

    // Build timeline: for each slot, use bar if exists, else placeholder
    return slots
      .map((slotTs) => {
        const ts = Number(slotTs);
        if (!Number.isFinite(ts)) return null;

        const bar = barMap.get(ts);
        if (bar) {
          return bar;
        } else {
          // Placeholder for missing slot
          return {
            date: ts,
            open: baselineClose,
            high: baselineClose,
            low: baselineClose,
            close: baselineClose,
            volume: 0,
            isFuture: true,
          };
        }
      })
      .filter((b) => b !== null);
  }, [bars, slots]);

  // Fast lookup from bar timestamp to its index within the timeline (used to realign saved orders)
  const dateToIndex = useMemo(() => {
    const map = new Map();
    timelineBars.forEach((bar, idx) => {
      if (bar && Number.isFinite(bar.date)) {
        map.set(Number(bar.date), idx);
      }
    });
    return map;
  }, [timelineBars]);

  // Resolve the closest bar index for a given timestamp (exact match preferred, otherwise nearest)
  const findIndexByTimestamp = useCallback(
    (ts) => {
      const t = Number(ts);
      if (!Number.isFinite(t) || !timelineBars.length) return null;
      if (dateToIndex.has(t)) return dateToIndex.get(t);

      let bestIdx = null;
      let bestDiff = Infinity;
      for (let i = 0; i < timelineBars.length; i++) {
        const bar = timelineBars[i];
        if (!bar || !Number.isFinite(bar.date)) continue;
        const diff = Math.abs(bar.date - t);
        if (diff < bestDiff) {
          bestDiff = diff;
          bestIdx = i;
        }
      }
      return bestIdx;
    },
    [dateToIndex, timelineBars]
  );

  // Index of the latest actual bar (ignores future placeholders)
  const lastActualIndex = useMemo(() => {
    for (let i = timelineBars.length - 1; i >= 0; i--) {
      const b = timelineBars[i];
      if (b && !b.isFuture) return i;
    }
    return -1;
  }, [timelineBars]);

  // Clamp how far we can pan right so at least one real bar stays visible
  const minOffsetLimit = useMemo(() => {
    if (!timelineBars.length) return 0;

    const fullWidth = timelineBars.length * colStride;
    const futureMin = Math.min(0, chartAreaW - LEFT - RIGHT - fullWidth);

    if (lastActualIndex < 0) return futureMin;

    // Keep at least the last three real bars visible while allowing deeper pan into future slots.
    // Anchor on the 3rd-from-last actual bar (or first bar if fewer than 3).
    const anchorIndex = Math.max(0, lastActualIndex - 7);
    const keepAnchorVisible = -anchorIndex * colStride;

    // Choose the less-negative bound so the anchor (ensuring 3 bars) cannot scroll out of view
    return Math.max(futureMin, keepAnchorVisible);
  }, [
    timelineBars.length,
    colStride,
    chartAreaW,
    LEFT,
    RIGHT,
    lastActualIndex,
  ]);

  // Clear last auto-pan record when instrument changes so orders on the new symbol can pan
  useEffect(() => {
    lastPannedOrderIdRef.current = null;
  }, [selectedInstrumentId]);

  // If an external order is selected, clear the trade selector first
  useEffect(() => {
    if (!externalOrder || !selectorActive) return;
    setSelectorActive(false);
    setSelectorLevels({ entry: null, target: null, stop: null });
    setSelectorStep("entry");
    setSelectorError("");
    selectorDragRef.current = null;
  }, [externalOrder, selectorActive]);

  // Auto-pan to selected external order's range start so its levels are in view
  useEffect(() => {
    if (!externalOrder || !timelineBars.length) return;
    if (
      externalOrder.instrument_id &&
      String(externalOrder.instrument_id) !== String(selectedInstrumentId)
    )
      return;

    const orderKey =
      externalOrder.bracket_id ||
      externalOrder.stream_id ||
      `${externalOrder.symbol || ""}-${externalOrder.created_at || ""}`;

    if (lastPannedOrderIdRef.current === orderKey) return;

    const candidates = [
      findIndexByTimestamp(externalOrder.entry_start_ts),
      findIndexByTimestamp(externalOrder.target_start_ts),
      findIndexByTimestamp(externalOrder.stop_start_ts),
    ].filter((v) => v != null);

    if (!candidates.length) return;

    const startIdx = Math.min(...candidates);
    const targetX = clamp(chartAreaW * 0.3, LEFT, Math.max(LEFT, chartAreaW - RIGHT));
    const desiredOffset = targetX - LEFT - startIdx * colStride;
    const clamped = clamp(desiredOffset, minOffsetLimit, colStride * 2);
    setOffsetX(clamped);
    lastPannedOrderIdRef.current = orderKey;
  }, [
    externalOrder,
    timelineBars.length,
    selectedInstrumentId,
    findIndexByTimestamp,
    colStride,
    minOffsetLimit,
    LEFT,
    RIGHT,
    chartAreaW,
    setOffsetX,
  ]);

  /**
   * Auto-scroll Effect:
   * Monitors changes in bar count. If the array grows (e.g., a new bar arrives via WebSocket),
   * and the user is currently viewing the latest data (right-aligned), this automatically shifts
   * the view to keep the new bar visible.
   *
   * Logic:
   * 1. Detect if data length increased.
   * 2. Calculate "previous" valid right-edge offset.
   * 3. If current offsetX is close to that previous edge (within buffer), snap to new edge.
   */
  const prevBarLengthRef = useRef(timelineBars.length);
  useEffect(() => {
    // Simply track length changes; do not auto-shift the viewport when new bars arrive
    prevBarLengthRef.current = timelineBars.length;
  }, [timelineBars.length]);

  /**
   * Calculates the currently visible bar window and triggers lazy loading.
   * Uses pixel-space math to map screen viewport to bar indices.
   *
   * This is a critical performance optimization:
   * - Only visible bars are rendered (viewport culling)
   * - Triggers auto-fetch when user scrolls within 50 bars of history start
   * - Accounts for device pixel ratio, pan offset, and zoom level
   * - Returns indices with 2-bar buffer for smooth panning
   */
  const visibleRange = useMemo(() => {
    const barCount = timelineBars.length;

    // Guard: return empty range if no bars loaded
    if (!barCount)
      return { start: 0, end: 0, scaleStart: 0, scaleEnd: 0, xOff: 0 };

    // Leftmost position (prevents over-panning past first bar)
    const minimumOffset = minOffsetLimit;

    // Clamp pan offset to reasonable bounds with 2-bar buffer
    const constrainedOffset = clamp(
      offsetX,
      minimumOffset - colStride * 2,
      colStride * 2
    );

    // Account for subpixel rendering at different device pixel ratios
    const devicePixelRatio = getDPR();
    const pixelPadding = 1 / devicePixelRatio;
    const halfBarBodyWidth = barWidth / 2;

    // Inverse mapping: convert viewport pixels → column indices
    // Left edge: -offsetX is the pixel distance, subtract (LEFT + halfWidth), divide by stride
    const firstVisibleColumn = Math.floor(
      (-constrainedOffset - (LEFT + halfBarBodyWidth) + pixelPadding) /
        colStride
    );

    // Right edge: viewport width minus pan offset, minus right margin
    const lastVisibleColumn = Math.ceil(
      (chartAreaW -
        RIGHT -
        constrainedOffset +
        halfBarBodyWidth -
        pixelPadding) /
        colStride
    );

    // Clamp indices to valid range
    const maxStartIndex = Math.max(0, barCount - 1);

    // Rendering overscan to avoid pop-in at edges
    const start = clamp(firstVisibleColumn - 2, 0, maxStartIndex);
    const end = clamp(lastVisibleColumn + 2, 0, barCount);

    // Tight range strictly limited to what is actually on screen for Y-scaling
    let scaleStart = clamp(firstVisibleColumn, 0, maxStartIndex);
    let scaleEnd = clamp(lastVisibleColumn + 1, scaleStart + 1, barCount);
    if (scaleEnd <= scaleStart) scaleEnd = Math.min(barCount, scaleStart + 1);

    // Auto-load older bars when user scrolls within first 50 bars
    if (start < 50 && hasMoreData && !loadingMoreBars && !loadingBars) {
      loadMoreBars();
    }

    return { start, end, scaleStart, scaleEnd, xOff: constrainedOffset };
  }, [
    timelineBars.length,
    colStride,
    chartAreaW,
    offsetX,
    LEFT,
    RIGHT,
    barWidth,
    hasMoreData,
    loadingMoreBars,
    loadingBars,
    minOffsetLimit,
  ]);

  // Keep pan offset in a ref for stable pointer math without re-binding listeners
  const xOffRef = useRef(visibleRange.xOff);
  useEffect(() => {
    xOffRef.current = visibleRange.xOff;
  }, [visibleRange.xOff]);

  /**
   * Builds Y-axis scaling helpers for prices within the visible window.
   * Scans visible bars to find price extremes, then creates a linear mapping function.
   *
   * The y() function converts any price to its pixel Y position:
   * - Inverted axis: high price = low pixel (since canvas Y increases downward)
   * - Includes 5% padding above/below for visual breathing room
   * - Auto-adjusts scale when user pans/zooms to different price regions
   */
  const scale = useMemo(() => {
    const barCount = timelineBars.length;

    // Guard: return dummy scale if no bars or invalid range
    if (!barCount || visibleRange.scaleEnd <= visibleRange.scaleStart)
      return { y: () => TOP, min: 0, max: 1, range: 1 };

    // Scan visible bars to find price extremes
    let min = Infinity,
      max = -Infinity;
    for (
      let index = visibleRange.scaleStart;
      index < visibleRange.scaleEnd;
      index++
    ) {
      const bar = timelineBars[index];
      if (!bar) continue;
      const priceSource =
        bar.isFuture && latestActualBar ? latestActualBar : bar;
      if (priceSource.low < min) min = priceSource.low;
      if (priceSource.high > max) max = priceSource.high;
    }

    // Fall back to latest actual bar if visible window has only future placeholders
    if (!Number.isFinite(min) || !Number.isFinite(max)) {
      if (latestActualBar) {
        min = latestActualBar.low;
        max = latestActualBar.high;
      } else {
        min = 0;
        max = 1;
      }
    }

    // Prevent zero-height scale when all bars are at same price
    if (min === max) {
      min -= 1;
      max += 1;
    }

    // Add fixed pixel padding above and below for consistent fit
    // Convert desired pixel padding to price units
    const pixelPad = 60; // Increased padding to keep candles clear of overlay chips
    const pricePerPixel = (max - min) / (glHeight - TOP - BOTTOM);
    min -= pixelPad * pricePerPixel;
    max += pixelPad * pricePerPixel;

    const range = max - min;

    // Linear transform: price p → screen pixel y
    // Formula: y = TOP + (max - p) * canvas_height / price_range
    // Higher price = smaller y (inverted axis since canvas Y increases downward)
    const y = (p) => TOP + ((max - p) * (glHeight - TOP - BOTTOM)) / range;
    // Horizontal grid lines (price axis ticks)
    const gridLines = [];
    const step = choosePriceStep(range, timeframe);
    const firstGridPrice = Math.ceil(min / step) * step;
    for (let price = firstGridPrice; price <= max; price += step) {
      gridLines.push(y(price));
    }
    return { min, max, range, y, gridLines };
  }, [
    timelineBars,
    visibleRange.scaleStart,
    visibleRange.scaleEnd,
    glHeight,
    TOP,
    BOTTOM,
    latestActualBar,
  ]);

  // Keep latest scale values in a ref to avoid rebinding pointer listeners during pan
  const scaleRef = useRef(scale);
  useEffect(() => {
    scaleRef.current = scale;
  }, [scale]);

  /**
   * Precomputes per-bar Y positions for WebGL buffers.
   * Converts price values to pixel coordinates using the scale function.
   * This optimization avoids recalculating Y positions during WebGL rendering.
   */
  const mappedBars = useMemo(() => {
    // Guard: return empty array if no bars
    if (!timelineBars.length) return [];

    // Map each bar, adding pixel Y positions for all price points
    return timelineBars.map((bar) => {
      const priceSource =
        bar.isFuture && latestActualBar ? latestActualBar : bar;
      return {
        ...bar, // Keep original data (date, open, high, low, close, volume)
        yOpen: scale.y(priceSource.open) - TOP, // Opening price Y position (adjusted for top padding)
        yClose: scale.y(priceSource.close) - TOP, // Closing price Y position
        yHigh: scale.y(priceSource.high) - TOP, // High price Y position (top of wick)
        yLow: scale.y(priceSource.low) - TOP, // Low price Y position (bottom of wick)
      };
    });
  }, [timelineBars, scale.min, scale.max, glHeight, TOP, latestActualBar]); // Recompute when scale or bars change

  /**
   * WebGL rendering effect: draws bars, wicks, and volume bars using GPU instancing.
   * This is the main rendering loop for the chart's primary visual elements.
   *
   * Uses instanced rendering for performance:
   * - Each bar component (wick, body, volume) is a single instanced quad
   * - Position, size, and color data packed into Float32Arrays
   * - GPU renders thousands of bars in a single draw call
   *
   * Rendering order (back to front):
   * 1. Volume bars (bottom layer, semi-transparent)
   * 2. Wicks (thin lines, full opacity)
   * 3. Bar bodies (rectangles, full opacity)
   */
  useEffect(() => {
    const chartBaseCanvas = chartBaseRef.current; // Get WebGL canvas element
    if (!chartBaseCanvas) return; // Guard: skip if canvas not mounted

    // Initialize or reuse WebGL context (ensureGL handles context creation and program compilation)
    const gl = ensureGL(
      chartBaseCanvas,
      chartAreaW,
      glHeight,
      glRef,
      glResRef,
      vaoRef
    );
    if (!gl) return; // Guard: skip if WebGL not available

    // Get WebGL resources (shaders, buffers, uniforms) from ref
    const glr = glResRef.current;
    const vao = vaoRef.current; // Vertex Array Object for attribute bindings
    const barCount = mappedBars.length;
    const { start, end } = visibleRange; // Visible bar indices

    // Clear canvas with theme background color
    const bgRGB = hexToRgb(activeBg); // Convert hex color to RGB floats
    gl.clearColor(bgRGB[0], bgRGB[1], bgRGB[2], 1); // Set clear color
    gl.clear(gl.COLOR_BUFFER_BIT); // Clear the canvas

    // Guard: skip rendering if no bars or invalid range
    if (!barCount || end <= start) return;

    // Enable scissor test to clip rendering to chart area (prevents overflow)
    gl.enable(gl.SCISSOR_TEST);
    const dpr = getDPR(); // Device pixel ratio for high-DPI displays

    // Define scissor rectangle in framebuffer coordinates (scaled by DPR)
    const scX = 0, // Scissor X origin
      scY = 0; // Scissor Y origin
    const scW = Math.floor(chartAreaW * dpr); // Scissor width (physical pixels)
    const scH = Math.floor(glHeight * dpr); // Scissor height (physical pixels)
    gl.scissor(scX, scY, scW, scH); // Set scissor rectangle

    // Activate shader program and set uniform values
    gl.useProgram(glr.program); // Use compiled shader program
    gl.uniform2f(glr.uCanvas, chartAreaW, glHeight); // Pass canvas dimensions to shader

    // Calculate how many instances we need to render
    const visibleInstanceCount = end - start; // Number of visible bars
    const maxDrawableInstances = Math.min(
      visibleInstanceCount,
      glr.maxInstances
    ); // Cap at GPU limit

    // Allocate Float32Arrays for bar body instance data
    // Each array stores data for all instances, to be uploaded to GPU in one batch
    const bodyCenters = new Float32Array(maxDrawableInstances * 2); // X,Y center positions
    const bodyHalf = new Float32Array(maxDrawableInstances * 2); // Half-width, half-height
    const bodyColors = new Float32Array(maxDrawableInstances * 4); // RGBA colors

    // Allocate Float32Arrays for wick instance data
    const wickCenters = new Float32Array(maxDrawableInstances * 2);
    const wickHalf = new Float32Array(maxDrawableInstances * 2);
    const wickColors = new Float32Array(maxDrawableInstances * 4);

    // Allocate Float32Arrays for volume bar instance data
    const volCenters = new Float32Array(maxDrawableInstances * 2);
    const volHalf = new Float32Array(maxDrawableInstances * 2);
    const volColors = new Float32Array(maxDrawableInstances * 4);

    // Convert user-selected bar colors to RGB floats
    const upRGB = hexToRgb(upCol); // Bullish bar color (close >= open)
    const downRGB = hexToRgb(downCol); // Bearish bar color (close < open)

    // Round to nearest odd integer to ensure consistent pixel-aligned rendering
    let wickW = getDPR() * (1.0 * wickScale);
    wickW = Math.max(1, Math.round(wickW));
    if (wickW % 2 === 0) wickW += 1;

    // Instance counters for each layer (incremented as we add instances to buffers)
    let bodyInstanceIndex = 0, // Number of body instances added
      wickInstanceIndex = 0, // Number of wick instances added
      volumeInstanceIndex = 0; // Number of volume instances added

    // Viewport boundaries for frustum culling
    // Extended by wick width + 1 subpixel to ensure wicks are fully rendered at edges
    const viewportLeft = LEFT - wickW - 1 / dpr; // Left edge (extended for wick width)
    const viewportRight = chartAreaW - RIGHT + wickW + 1 / dpr; // Right edge (extended for wick width)

    // Find maximum volume in visible range (for volume bar height normalization)
    let maxVol = 0; // Will store the highest volume value
    if (volEnabled && VOLUME_H > 0) {
      // Scan visible bars to find peak volume
      for (let columnIndex = start; columnIndex < end; columnIndex++) {
        const bar = mappedBars[columnIndex];
        if (bar && bar.volume > maxVol) maxVol = bar.volume;
      }
      // Prevent division by zero if all volumes are 0
      if (maxVol <= 0) maxVol = 1;
    }
    const volDrawH = Math.max(0, VOLUME_H); // Drawable volume height (0 if disabled)

    // Loop through visible bars and build WebGL instance data buffers.
    // Each bar has wicks (high-low lines), body (open-close rectangle), and volume bar.
    // Geometry is instanced: position + half-size + color packed into Float32Arrays, then sent to GPU.
    for (let columnIndex = start; columnIndex < end; columnIndex++) {
      const bar = mappedBars[columnIndex];
      if (!bar || bar.isFuture) continue; // Skip placeholders; they only reserve timeline slots

      // Screen X position of bar center, accounting for pan offset and zoom
      const barCenterX =
        LEFT + visibleRange.xOff + columnIndex * colStride + colStride / 2;

      // Wick: thin line from low to high, color depends on bar direction
      const isWickUp = bar.close >= bar.open;
      const wickColor = isWickUp ? upRGB : downRGB;
      // Use bar body width for culling, not just wick width
      const bodyLeft = barCenterX - barWidth / 2;
      const bodyRight = barCenterX + barWidth / 2;

      // Only render wick if bar body would be visible in viewport and we have capacity
      if (
        bodyRight > viewportLeft &&
        bodyLeft < viewportRight &&
        wickInstanceIndex < maxDrawableInstances
      ) {
        // Wick Y center is midpoint between high and low prices
        const wickCenterY = (bar.yLow + bar.yHigh) * 0.5;

        // Store center (X, Y) in floating point
        wickCenters[wickInstanceIndex * 2] = barCenterX;
        wickCenters[wickInstanceIndex * 2 + 1] = wickCenterY;

        // Store half-dimensions: GPU shader draws 2x this size centered at center position
        wickHalf[wickInstanceIndex * 2] = Math.max(0.5, wickW * 0.5);
        wickHalf[wickInstanceIndex * 2 + 1] = Math.max(
          0.5,
          Math.abs(bar.yHigh - bar.yLow) * 0.5
        );

        // Store RGBA color (4 components per instance)
        wickColors[wickInstanceIndex * 4] = wickColor[0];
        wickColors[wickInstanceIndex * 4 + 1] = wickColor[1];
        wickColors[wickInstanceIndex * 4 + 2] = wickColor[2];
        wickColors[wickInstanceIndex * 4 + 3] = 1.0; // Full opacity

        wickInstanceIndex++;
      }

      // --- Bar Body Calculation ---
      // Determine physical bounds of the bar body (Open to Close range)
      // Math.min/max ensures consistent top/bottom regardless of bar direction (bull/bear)
      const barBodyTop = Math.min(bar.yOpen, bar.yClose);
      const barBodyBottom = Math.max(bar.yOpen, bar.yClose);
      const bodyCenterY = (barBodyTop + barBodyBottom) * 0.5;

      const isBodyUp = bar.close >= bar.open;
      const bodyColor = isBodyUp ? upRGB : downRGB;

      // --- Body Instance Population ---
      // Only draw bodies when zoomed in enough (width >= threshold);
      // otherwise, render wicks-only for better clarity at high density.
      if (
        barWidth >= SHOW_BODY_THRESHOLD &&
        bodyInstanceIndex < maxDrawableInstances
      ) {
        // Position: X, Y Center
        bodyCenters[bodyInstanceIndex * 2] = barCenterX;
        bodyCenters[bodyInstanceIndex * 2 + 1] = bodyCenterY;

        // Dimensions: Half-width, Half-height (passed to shader)
        // Ensure at least 0.5px size to prevent aliasing disappearance
        bodyHalf[bodyInstanceIndex * 2] = Math.max(0.5, barWidth * 0.5);
        bodyHalf[bodyInstanceIndex * 2 + 1] = Math.max(
          0.5,
          (barBodyBottom - barBodyTop) * 0.5
        );

        // Color: R, G, B, Alpha
        bodyColors[bodyInstanceIndex * 4] = bodyColor[0];
        bodyColors[bodyInstanceIndex * 4 + 1] = bodyColor[1];
        bodyColors[bodyInstanceIndex * 4 + 2] = bodyColor[2];
        bodyColors[bodyInstanceIndex * 4 + 3] = 1.0; // Full opacity

        bodyInstanceIndex++;
      }

      // --- Volume Bar Calculation ---
      // Renders volume histogram at the bottom of the chart
      if (
        volEnabled &&
        VOLUME_H > 0 &&
        volumeInstanceIndex < maxDrawableInstances
      ) {
        // Calculate relative height based on max volume in view
        const volumeRatio = Math.min(1, bar.volume / maxVol);
        const volumeHeight = Math.max(1, volDrawH * volumeRatio);

        const volumeGap = 1; // 1px gap between adjacent bars
        const volumeWidth = Math.max(1, colStride - volumeGap);

        // Position volume bars in the dedicated volume area at bottom
        const volumeBaseY = chartAreaH;
        // Instanced quads are drawn from center, so offset Y by half height
        const volumeCenterY =
          volumeBaseY + (volDrawH - volumeHeight) + volumeHeight / 2;

        // Populate Volume Instance Buffers
        volCenters[volumeInstanceIndex * 2] = barCenterX;
        volCenters[volumeInstanceIndex * 2 + 1] = volumeCenterY;

        volHalf[volumeInstanceIndex * 2] = volumeWidth / 2;
        volHalf[volumeInstanceIndex * 2 + 1] = volumeHeight / 2;

        // Use same color as bar body but with reduced opacity
        volColors[volumeInstanceIndex * 4] = bodyColor[0];
        volColors[volumeInstanceIndex * 4 + 1] = bodyColor[1];
        volColors[volumeInstanceIndex * 4 + 2] = bodyColor[2];
        volColors[volumeInstanceIndex * 4 + 3] = 0.6; // 60% opacity

        volumeInstanceIndex++;
      }
    }

    // Bind Vertex Array Object (contains attribute pointer configuration)
    gl.bindVertexArray(vao);

    // --- GRID LINES (BEHIND BARS/VOLUME) ---
    // Horizontal grid lines (price axis)
    const gridLineColor = hexToRgb(activeGrid);
    const gridLineAlpha = 1.0;
    const gridCenters = new Float32Array(scale.gridLines.length * 2);
    const gridHalf = new Float32Array(scale.gridLines.length * 2);
    const gridColors = new Float32Array(scale.gridLines.length * 4);
    for (let i = 0; i < scale.gridLines.length; i++) {
      gridCenters[i * 2] = chartAreaW / 2;
      gridCenters[i * 2 + 1] = scale.gridLines[i];
      // Span entire drawable width (no left/right padding cut-off)
      gridHalf[i * 2] = chartAreaW / 2;
      gridHalf[i * 2 + 1] = 0.5;
      gridColors[i * 4] = gridLineColor[0];
      gridColors[i * 4 + 1] = gridLineColor[1];
      gridColors[i * 4 + 2] = gridLineColor[2];
      gridColors[i * 4 + 3] = gridLineAlpha;
    }
    if (scale.gridLines.length > 0) {
      gl.bindBuffer(gl.ARRAY_BUFFER, glr.instCenter);
      gl.bufferData(gl.ARRAY_BUFFER, gridCenters, gl.DYNAMIC_DRAW);
      bindInst(gl, glr.instCenter, glr.iCenter, 2);
      gl.bindBuffer(gl.ARRAY_BUFFER, glr.instHalf);
      gl.bufferData(gl.ARRAY_BUFFER, gridHalf, gl.DYNAMIC_DRAW);
      bindInst(gl, glr.instHalf, glr.iHalfSize, 2);
      gl.bindBuffer(gl.ARRAY_BUFFER, glr.instColor);
      gl.bufferData(gl.ARRAY_BUFFER, gridColors, gl.DYNAMIC_DRAW);
      bindInst(gl, glr.instColor, glr.iColor, 4);
      gl.drawArraysInstanced(gl.TRIANGLES, 0, 6, scale.gridLines.length);
    }

    // Vertical grid lines (time axis labels)
    // Vertical grid lines (time axis labels) - align with labelAnchorXsRef
    const labelAnchorXs = labelAnchorXsRef.current || [];
    const vGridCenters = new Float32Array(labelAnchorXs.length * 2);
    const vGridHalf = new Float32Array(labelAnchorXs.length * 2);
    const vGridColors = new Float32Array(labelAnchorXs.length * 4);
    for (let i = 0; i < labelAnchorXs.length; i++) {
      vGridCenters[i * 2] = labelAnchorXs[i];
      vGridCenters[i * 2 + 1] = glHeight / 2;
      vGridHalf[i * 2] = 0.5;
      // Span full chart height (no top/bottom padding cut-off)
      vGridHalf[i * 2 + 1] = glHeight / 2;
      vGridColors[i * 4] = gridLineColor[0];
      vGridColors[i * 4 + 1] = gridLineColor[1];
      vGridColors[i * 4 + 2] = gridLineColor[2];
      vGridColors[i * 4 + 3] = gridLineAlpha;
    }
    if (labelAnchorXs.length > 0) {
      gl.bindBuffer(gl.ARRAY_BUFFER, glr.instCenter);
      gl.bufferData(gl.ARRAY_BUFFER, vGridCenters, gl.DYNAMIC_DRAW);
      bindInst(gl, glr.instCenter, glr.iCenter, 2);
      gl.bindBuffer(gl.ARRAY_BUFFER, glr.instHalf);
      gl.bufferData(gl.ARRAY_BUFFER, vGridHalf, gl.DYNAMIC_DRAW);
      bindInst(gl, glr.instHalf, glr.iHalfSize, 2);
      gl.bindBuffer(gl.ARRAY_BUFFER, glr.instColor);
      gl.bufferData(gl.ARRAY_BUFFER, vGridColors, gl.DYNAMIC_DRAW);
      bindInst(gl, glr.instColor, glr.iColor, 4);
      gl.drawArraysInstanced(gl.TRIANGLES, 0, 6, labelAnchorXs.length);
    }

    // --- END GRID LINES ---

    // Render volume bars first (bottom layer, drawn behind bars)
    if (volEnabled && volumeInstanceIndex > 0) {
      // Upload volume center positions to GPU
      gl.bindBuffer(gl.ARRAY_BUFFER, glr.instCenter); // Bind center position buffer
      gl.bufferData(
        gl.ARRAY_BUFFER,
        volCenters.subarray(0, volumeInstanceIndex * 2), // Only upload used portion
        gl.DYNAMIC_DRAW // Hint: data changes frequently
      );
      bindInst(gl, glr.instCenter, glr.iCenter, 2); // Configure attribute pointer (2 floats per instance)

      // Upload volume half-dimensions to GPU
      gl.bindBuffer(gl.ARRAY_BUFFER, glr.instHalf); // Bind half-size buffer
      gl.bufferData(
        gl.ARRAY_BUFFER,
        volHalf.subarray(0, volumeInstanceIndex * 2),
        gl.DYNAMIC_DRAW
      );
      bindInst(gl, glr.instHalf, glr.iHalfSize, 2); // 2 floats per instance (half-width, half-height)

      // Upload volume colors to GPU
      gl.bindBuffer(gl.ARRAY_BUFFER, glr.instColor); // Bind color buffer
      gl.bufferData(
        gl.ARRAY_BUFFER,
        volColors.subarray(0, volumeInstanceIndex * 4), // 4 floats per instance (RGBA)
        gl.DYNAMIC_DRAW
      );
      bindInst(gl, glr.instColor, glr.iColor, 4); // 4 floats per instance

      // Draw all volume bar instances in one call
      gl.drawArraysInstanced(gl.TRIANGLES, 0, 6, volumeInstanceIndex); // 6 vertices (2 triangles) per instance
    }

    // Render wicks (middle layer, thin lines from high to low)
    if (wickInstanceIndex > 0) {
      // Upload wick center positions
      gl.bindBuffer(gl.ARRAY_BUFFER, glr.instCenter);
      gl.bufferData(
        gl.ARRAY_BUFFER,
        wickCenters.subarray(0, wickInstanceIndex * 2),
        gl.DYNAMIC_DRAW
      );
      bindInst(gl, glr.instCenter, glr.iCenter, 2);

      // Upload wick half-dimensions
      gl.bindBuffer(gl.ARRAY_BUFFER, glr.instHalf);
      gl.bufferData(
        gl.ARRAY_BUFFER,
        wickHalf.subarray(0, wickInstanceIndex * 2),
        gl.DYNAMIC_DRAW
      );
      bindInst(gl, glr.instHalf, glr.iHalfSize, 2);

      // Upload wick colors
      gl.bindBuffer(gl.ARRAY_BUFFER, glr.instColor);
      gl.bufferData(
        gl.ARRAY_BUFFER,
        wickColors.subarray(0, wickInstanceIndex * 4),
        gl.DYNAMIC_DRAW
      );
      bindInst(gl, glr.instColor, glr.iColor, 4);

      // Draw all wick instances
      gl.drawArraysInstanced(gl.TRIANGLES, 0, 6, wickInstanceIndex);
    }

    // Render bar bodies (top layer, rectangles from open to close)
    if (bodyInstanceIndex > 0) {
      // Upload body center positions
      gl.bindBuffer(gl.ARRAY_BUFFER, glr.instCenter);
      gl.bufferData(
        gl.ARRAY_BUFFER,
        bodyCenters.subarray(0, bodyInstanceIndex * 2),
        gl.DYNAMIC_DRAW
      );
      bindInst(gl, glr.instCenter, glr.iCenter, 2);

      // Upload body half-dimensions
      gl.bindBuffer(gl.ARRAY_BUFFER, glr.instHalf);
      gl.bufferData(
        gl.ARRAY_BUFFER,
        bodyHalf.subarray(0, bodyInstanceIndex * 2),
        gl.DYNAMIC_DRAW
      );
      bindInst(gl, glr.instHalf, glr.iHalfSize, 2);

      // Upload body colors
      gl.bindBuffer(gl.ARRAY_BUFFER, glr.instColor);
      gl.bufferData(
        gl.ARRAY_BUFFER,
        bodyColors.subarray(0, bodyInstanceIndex * 4),
        gl.DYNAMIC_DRAW
      );
      bindInst(gl, glr.instColor, glr.iColor, 4);

      // Draw all body instances
      gl.drawArraysInstanced(gl.TRIANGLES, 0, 6, bodyInstanceIndex);
    }

    // Cleanup WebGL state
    gl.disable(gl.SCISSOR_TEST); // Disable scissor test
    gl.bindVertexArray(null); // Unbind VAO
  }, [
    chartAreaW,
    glHeight,
    mappedBars,
    visibleRange.start,
    visibleRange.end,
    visibleRange.xOff,
    barWidth,
    colStride,
    upCol,
    downCol,
    activeBg,
    TOP,
    BOTTOM,
    LEFT,
    RIGHT,
    wickScale,
    volEnabled,
    VOLUME_H,
    chartAreaH,
    labelGridVersion, // <-- Add this dependency
  ]);

  /**
   * Renders time scale grid and labels (static layer).
   * Draws vertical grid lines and time/date labels at regular intervals along X-axis.
   *
   * Label spacing is adaptive based on zoom level:
   * - Very zoomed out (bar < 6px): 120px gap between labels
   * - Zoomed in (bar >= 64px): 40px gap between labels
   *
   * Special handling for daily timeframe:
   * - Bold labels for January (start of fiscal year)
   * - Includes year display for multi-year views
   */
  useEffect(() => {
    const timeBaseCanvas = timeBaseRef.current; // Get time scale canvas element
    if (!timeBaseCanvas) return; // Guard: skip if not mounted

    // Prepare 2D context: clear, set size, and cache for reuse
    const timeBaseContext = prepCanvas2D(
      timeBaseCanvas,
      chartAreaW,
      TIME_SCALE_H
    );

    // 1. Fill background with theme color
    timeBaseContext.fillStyle = activeBg;
    timeBaseContext.fillRect(0, 0, chartAreaW, TIME_SCALE_H);

    // 2. Configure text style for Time Labels
    timeBaseContext.globalAlpha = 1;
    timeBaseContext.fillStyle = colors.textColor; // Label text color
    timeBaseContext.font = "13px 'JetBrains Mono', 'Fira Code', monospace"; // Monospace font
    timeBaseContext.textAlign = "center"; // Center-align labels
    timeBaseContext.textBaseline = "top"; // Align to top

    // 4. Calculate adaptive label interval
    // Determine how many bars to skip between labels based on current zoom (barWidth).
    // The goal is to maintain a roughly constant visual gap (e.g., 120px when zoomed out, 40px when zoomed in).
    const barCount = mappedBars.length;
    let labelInterval; // Number of bars to skip between labels
    let minimumLabelGap = 60; // Minimum pixels between adjacent labels
    let dayStride = null; // For 1D smart month/day labels
    let showDayLabels = true; // Toggle day numbers when zoomed in enough
    let minuteAnchorInterval = null; // For 1m: anchor at hour/15/30/45 min marks
    let showMinuteLabels = true; // Toggle minute labels when zoomed in
    let minuteStride = null; // For 1m: stride for minute fill labels (5, 10, etc.)

    if (timeframe === "1D" && !is_intraday) {
      // Scan all bars; determine stride dynamically to pack days between month markers
      labelInterval = 1;
      const pxPerDay = colStride;
      // Target ~90px between day labels; clamp stride for sanity
      dayStride = Math.max(2, Math.min(21, Math.round(90 / Math.max(pxPerDay, 1))))
        || 3; // Ensure at least 2 day gap
      minimumLabelGap = 64;
      // When heavily zoomed out (very tight pixels per day), hide day numbers and show only months/years
      showDayLabels = colStride >= 24;
    } else if (timeframe === "1m" || is_intraday) {
      // Smart minute labeling with hour/quarter-hour anchors
      labelInterval = 1;
      const pxPerMinute = colStride;
      minimumLabelGap = 60;
      
      // Determine anchor interval and minute stride based on zoom level
      // Very zoomed out: hour marks (60 min)
      // Medium: 15-minute marks
      // Zoomed in: show more minute labels between anchors
      if (colStride < 2) {
        minuteAnchorInterval = 60; // Hour marks only
        showMinuteLabels = false;
      } else if (colStride < 4) {
        minuteAnchorInterval = 30; // Half-hour marks
        showMinuteLabels = false;
      } else if (colStride < 8) {
        minuteAnchorInterval = 30; // Quarter-hour marks
        showMinuteLabels = false;
      } else {
        // Maximum label frequency: 5 minutes (prevents jitter at all zoom levels)
        minuteAnchorInterval = 30; // Quarter-hour anchors
        minuteStride = 10; // Always maintain 10-minute spacing
        showMinuteLabels = true;
      }
    } else {
      if (barWidth < 6)
        labelInterval = Math.max(1, Math.floor(120 / colStride));
      else if (barWidth < 12)
        labelInterval = Math.max(1, Math.floor(80 / colStride));
      else if (barWidth < 20)
        labelInterval = Math.max(1, Math.floor(60 / colStride));
      else labelInterval = Math.max(1, Math.floor(40 / colStride));
    }

    // 5. Render Labels
    // Walk across visible bars, laying down labels every `labelInterval` while ensuring they
    // stay on screen and do not collide with a previously drawn label.
    let previousLabelPositionX = -Infinity; // Track last label X to prevent overlap
    const baseMinGap = minimumLabelGap; // Minimum pixels between adjacent labels
    const anchorMonthDate = barCount > 0 ? new Date(mappedBars[0].date) : null;
    const anchorMonthIndex = anchorMonthDate
      ? anchorMonthDate.getFullYear() * 12 + anchorMonthDate.getMonth()
      : 0;
    const monthCadence =
      timeframe === "1D" && !is_intraday
        ? Math.max(1, Math.ceil(baseMinGap / Math.max(colStride * 30, 1)))
        : 1;

    // Collect label anchor Xs for gridlines (only for actually drawn labels)
    const labelAnchorXs = [];
    for (
      let barIndex = visibleRange.start;
      barIndex < visibleRange.end;
      barIndex += labelInterval
    ) {
      if (barIndex >= barCount) break;
      const labelPositionX = LEFT + visibleRange.xOff + barIndex * colStride + colStride / 2;
      if (labelPositionX < LEFT || labelPositionX > chartAreaW - RIGHT) continue;
      const barDateTs = mappedBars[barIndex].date;
      const prevDateTs = barIndex > 0 ? mappedBars[barIndex - 1].date : null;
      const nextDateTs = barIndex + 1 < barCount ? mappedBars[barIndex + 1].date : null;
      const barDate = new Date(barDateTs);
      const prevDate = prevDateTs ? new Date(prevDateTs) : null;
      const nextDate = nextDateTs ? new Date(nextDateTs) : null;
      const monthChanged = !prevDate || prevDate.getMonth() !== barDate.getMonth() || prevDate.getFullYear() !== barDate.getFullYear();
      const nextMonthChanged = nextDate && (nextDate.getMonth() !== barDate.getMonth() || nextDate.getFullYear() !== barDate.getFullYear());
      if (labelPositionX - previousLabelPositionX < baseMinGap) continue;
      let label = "";
      if (timeframe === "1D" && !is_intraday) {
        if (monthChanged) {
          const monthIndex = barDate.getFullYear() * 12 + barDate.getMonth();
          const monthAligned = monthCadence === 1 || (monthIndex - anchorMonthIndex) % monthCadence === 0;
          if (!monthAligned) continue;
          label = formatMonthLabel(barDate);
        } else {
          if (!showDayLabels) continue;
          if (nextMonthChanged && colStride < baseMinGap * 0.9) continue;
          if (dayStride && barDate.getDate() % dayStride !== 0) continue;
          label = String(barDate.getDate());
        }
      } else if (timeframe === "1m" || is_intraday) {
        const minute = barDate.getMinutes();
        const hour = barDate.getHours();
        const prevMinute = prevDate ? prevDate.getMinutes() : null;
        const prevHour = prevDate ? prevDate.getHours() : null;
        const largeGap = prevDateTs && (barDateTs - prevDateTs) > 10 * 60 * 60 * 1000;
        const nextLargeGap = nextDateTs && (nextDateTs - barDateTs) > 10 * 60 * 60 * 1000;
        const isHourBoundary = minute === 0 && (prevMinute !== 0 || prevHour !== hour);
        const isQuarterBoundary = minuteAnchorInterval <= 15 && (minute % 15 === 0) && (prevMinute == null || prevMinute % 15 !== 0 || prevHour !== hour);
        const isHalfBoundary = minuteAnchorInterval === 30 && (minute % 30 === 0) && (prevMinute == null || prevMinute % 30 !== 0 || prevHour !== hour);
        const isAnchor = isHourBoundary || isQuarterBoundary || isHalfBoundary || largeGap;
        // Skip labeling the last bar of a session; defer the label to the first bar after the gap (e.g., 09:15 next day)
        if (nextLargeGap) continue;
        if (isAnchor) {
          if (largeGap) {
            label = barDate.toLocaleDateString("en-US", { month: "short", day: "2-digit" }) + " " + barDate.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit", hour12: false });
          } else if (minute === 0) {
            label = barDate.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit", hour12: false });
          } else {
            label = barDate.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit", hour12: false });
          }
        } else {
          if (!showMinuteLabels) continue;
          if (minuteStride && minute % minuteStride !== 0) continue;
          const nextMinute = nextDate ? nextDate.getMinutes() : null;
          const nextHour = nextDate ? nextDate.getHours() : null;
          const nextIsAnchor = nextLargeGap || (nextMinute === 0) || (minuteAnchorInterval <= 15 && nextMinute % 15 === 0 && nextHour !== hour) || (minuteAnchorInterval === 30 && nextMinute % 30 === 0 && nextHour !== hour);
          if (nextIsAnchor && colStride < baseMinGap * 0.9) continue;
          label = String(minute).padStart(2, "0");
        }
      } else {
        label = formatTimeLabel(mappedBars[barIndex].date, timeframe, barWidth, is_intraday);
      }
      drawAxisLabelText(timeBaseContext, label, Math.floor(labelPositionX), 5);
      labelAnchorXs.push(labelPositionX);
      previousLabelPositionX = labelPositionX;
    }
    // Store label anchor Xs for use in WebGL gridline rendering
    labelAnchorXsRef.current = labelAnchorXs;
    // Trigger gridline re-render after label Xs are set
    setLabelGridVersion((v) => v + 1);
  }, [
    chartAreaW,
    TIME_SCALE_H,
    activeBg,
    activeGrid,
    activeText,
    mappedBars,
    visibleRange.start,
    visibleRange.end,
    visibleRange.xOff,
    colStride,
    barWidth,
    timeframe,
    is_intraday,
    LEFT,
    RIGHT,
    colors,
  ]);

  /**
   * Renders time scale hover overlay (interactive layer).
   * Shows timestamp/date chip when user hovers over a bar.
   * Displays format depends on timeframe:
   * - Intraday/1m-1h: "Mon DD HH:MM:SS"
   * - Daily+: "Mon DD YYYY"
   */
  useEffect(() => {
    const timeOverlayCanvas = timeOverlayRef.current; // Get overlay canvas
    if (!timeOverlayCanvas) return; // Guard: skip if not mounted

    // Prepare 2D context
    const timeOverlayContext = prepCanvas2D(
      timeOverlayCanvas,
      chartAreaW,
      TIME_SCALE_H
    );

    // Clear canvas (removes previous hover state)
    timeOverlayContext.clearRect(0, 0, chartAreaW, TIME_SCALE_H);

    // Guard: skip rendering if no hover state or no data
    if (hoverIndex == null || mappedBars.length === 0) return;

    // Calculate X position of hover marker (center of hovered bar)
    const hoverMarkerX =
      LEFT + visibleRange.xOff + hoverIndex * colStride + colStride / 2;

    // Skip rendering if marker is outside viewport
    if (hoverMarkerX < 0 || hoverMarkerX > chartAreaW) return;
    const hoverDate = new Date(mappedBars[hoverIndex].date);
    const hoverLabel =
      is_intraday || ["1m", "5m", "15m", "30m", "1h"].includes(timeframe)
        ? hoverDate.toLocaleDateString("en-US", {
            month: "short",
            day: "2-digit",
          }) +
          " " +
          hoverDate.toLocaleTimeString("en-US", {
            hour: "2-digit",
            minute: "2-digit",
            hour12: false,
          })
        : hoverDate.toLocaleDateString("en-US", {
            month: "short",
            day: "2-digit",
          });
    // Size the chip: measure text, add horizontal padding, enforce a sane minimum width so
    // very short labels (e.g., single-digit day) still look balanced.
    const padding = 9;
    timeOverlayContext.font = "14px 'JetBrains Mono', 'Fira Code', monospace";
    const labelWidth = Math.max(
      102, // Minimum chip width for consistent UI
      Math.ceil(timeOverlayContext.measureText(hoverLabel).width) + padding * 2
    );
    const labelHeight = 28;
    // Center the chip horizontally on the hovered bar and pin it near the top of the axis.
    // Clamp X position to prevent chip from clipping at left/right edges
    const labelRectX = clamp(
      Math.floor(hoverMarkerX - labelWidth / 2),
      2,
      chartAreaW - labelWidth - 2
    );
    const labelRectY = 2;
    // Draw chip background with rounded corners
    timeOverlayContext.fillStyle = colors.chipBg; // Background color
    timeOverlayContext.strokeStyle = colors.chipBorder; // Border color
    timeOverlayContext.lineWidth = 1.5; // Border width
    roundRectPath(
      timeOverlayContext,
      labelRectX,
      labelRectY,
      labelWidth,
      labelHeight,
      6 // Corner radius
    );
    timeOverlayContext.fill(); // Fill background
    timeOverlayContext.stroke(); // Draw border

    // Draw label text centered in chip
    timeOverlayContext.fillStyle = colors.chipText; // Text color
    timeOverlayContext.textAlign = "center"; // Center horizontally
    timeOverlayContext.textBaseline = "middle"; // Center vertically
    drawAxisLabelText(
      timeOverlayContext,
      hoverLabel,
      labelRectX + labelWidth / 2, // X center
      labelRectY + labelHeight / 2 + 0.5 // Y center (+0.5 for pixel-perfect alignment)
    );
  }, [
    chartAreaW,
    TIME_SCALE_H,
    hoverIndex,
    mappedBars,
    visibleRange.xOff,
    colStride,
    timeframe,
    is_intraday,
    LEFT,
    colors,
  ]);

  /**
   * Renders price scale grid and labels (static layer).
   * Draws horizontal grid lines and price tick labels at regular intervals along Y-axis.
   *
   * Uses adaptive tick spacing based on price range:
   * - Large ranges (> 1000): ticks at 100s or 1000s
   * - Medium ranges (> 100): ticks at 10s
   * - Small ranges: ticks at 1s or 0.1s
   */
  useEffect(() => {
    const priceBaseCanvas = priceBaseRef.current; // Get price scale canvas
    if (!priceBaseCanvas) return; // Guard: skip if not mounted

    // Prepare 2D context
    const priceBaseContext = prepCanvas2D(
      priceBaseCanvas,
      PRICE_SCALE_W,
      glHeight
    );

    // Fill background with theme color
    priceBaseContext.fillStyle = activeBg;
    priceBaseContext.fillRect(0, 0, PRICE_SCALE_W, glHeight);

    // Calculate smart tick spacing based on price range
    const baseStep = choosePriceStep(scale.range, timeframe); // Choose tick interval (1, 10, 100, etc.)
    const step = Math.max(baseStep, tickSize || 0); // Align to tick size when available
    const dec = decimalsForStep(step); // Decimal places to show (depends on step size)

    // Generate ticks aligned to round numbers (TradingView-style)
    // Start from the first round number >= scale.min
    const firstTick = Math.ceil(scale.min / step) * step;
    const ticks = [];
    for (let price = firstTick; price <= scale.max; price += step) {
      // Round to avoid floating point errors
      const roundedPrice = Math.round(price / step) * step;
      ticks.push(roundedPrice);
    }

    // Reset alpha for text rendering
    priceBaseContext.globalAlpha = 1;
    priceBaseContext.fillStyle = activeText; // Label text color
    priceBaseContext.font = "13px 'JetBrains Mono', 'Fira Code', monospace";
    priceBaseContext.textAlign = "center"; // Center text horizontally
    priceBaseContext.textBaseline = "middle"; // Center text vertically

    // Draw price labels at each tick
    for (const priceValue of ticks) {
      // Calculate Y position for this price
      const priceLabelY = scale.y(priceValue) - TOP;

      // Skip if off-screen
      if (priceLabelY < 0 || priceLabelY > glHeight) continue;

      // Draw formatted price label
      drawAxisLabelText(
        priceBaseContext,
        formatPrice(priceValue, dec), // Format with appropriate decimals
        PRICE_SCALE_W / 2, // X center of scale
        Math.floor(priceLabelY) // Y position (floored for crisp alignment)
      );
    }
  }, [
    PRICE_SCALE_W,
    glHeight,
    activeBg,
    activeGrid,
    activeText,
    scale.min,
    scale.max,
    scale.range,
    tickSize,
  ]);

  /**
   * Renders price scale hover overlay (interactive layer).
   * Shows two types of chips:
   * 1. Live price chip: always visible (latest bar close price)
   * 2. Hover price chip: visible when user moves cursor vertically within chart
   *
   * Uses inverse of scale.y() function to convert mouse Y position back to price.
   */
  useEffect(() => {
    const priceOverlayCanvas = priceOverlayRef.current; // Get overlay canvas
    if (!priceOverlayCanvas) return; // Guard: skip if not mounted

    // Prepare 2D context
    const priceOverlayContext = prepCanvas2D(
      priceOverlayCanvas,
      PRICE_SCALE_W,
      glHeight
    );

    // Clear canvas (removes previous overlays)
    priceOverlayContext.clearRect(0, 0, PRICE_SCALE_W, glHeight);

    // SECTION 1: Draw live price chip (always visible if we have data)
    // Displays the current market price based on the last available bar.
    if (mappedBars.length > 0) {
      let mostRecentBar = latestActualBar;

      if (!mostRecentBar) {
        for (let i = mappedBars.length - 1; i >= 0; i--) {
          const candidate = mappedBars[i];
          if (candidate && !candidate.isFuture) {
            mostRecentBar = candidate;
            break;
          }
        }
      }

      if (!mostRecentBar) return; // No actual data to anchor live price
      const isPriceUp = mostRecentBar.close >= mostRecentBar.open; // Bullish?
      const livePriceColor = isPriceUp ? upCol : downCol; // Color based on direction
      const livePrice = mostRecentBar.close; // Current price
      const livePriceY = scale.y(livePrice) - TOP; // Y pixel position (mapped from price)
      const roundedLivePriceY = Math.round(livePriceY); // Round for crisp pixel alignment

      // Only render if price is within the visible vertical range
      if (livePriceY >= 0 && livePriceY <= glHeight) {
        // Determine decimal precision based on price magnitude
        const step = choosePriceStep(scale.range, timeframe);
        const decimalPlaces = Math.max(2, decimalsForStep(step));
        const livePriceLabel = formatPrice(livePrice, decimalPlaces);

        // Calculate chip layout based on text width
        const padding = 6;
        priceOverlayContext.font =
          "13px 'JetBrains Mono', 'Fira Code', monospace";
        const chipWidth = Math.max(
          52, // Minimum width
          Math.ceil(priceOverlayContext.measureText(livePriceLabel).width) +
            padding * 2
        );
        const chipHeight = 20;
        const chipX = Math.floor((PRICE_SCALE_W - chipWidth) / 2); // Center horizontally

        // Clamp Y position to prevent chip from clipping at top/bottom edges
        const chipY = clamp(
          roundedLivePriceY - chipHeight / 2,
          2,
          glHeight - chipHeight - 2
        );

        // Draw solid colored background (Green/Red) to highlight current price
        priceOverlayContext.fillStyle = livePriceColor;
        priceOverlayContext.strokeStyle = livePriceColor;
        priceOverlayContext.lineWidth = 1.5;
        roundRectPath(
          priceOverlayContext,
          chipX,
          chipY,
          chipWidth,
          chipHeight,
          4
        );
        priceOverlayContext.fill();
        priceOverlayContext.stroke();

        // Draw white text for high contrast against colored background
        priceOverlayContext.fillStyle = colors.priceChipText;
        priceOverlayContext.textAlign = "center";
        priceOverlayContext.textBaseline = "middle";
        drawAxisLabelText(
          priceOverlayContext,
          livePriceLabel,
          chipX + chipWidth / 2,
          chipY + chipHeight / 2 + 0.5 // Subpixel vertical center
        );
      }
    }

    // SECTION 2: Draw hover price chip (follows cursor Y).
    // Converts pixel Y back to a price using the inverse of the y() scale.
    if (mouseYInner == null) return;
    const clampedY = clamp(mouseYInner, 0, glHeight); // Keep calculation inside canvas bounds

    // Inverse mapping math:
    // Standard Y = TOP + ((Max - Price) * EffectiveHeight) / Range
    // Solve for Price: Price = Max - ((Y - TOP) * Range) / EffectiveHeight
    const effectiveHeight = glHeight - TOP - BOTTOM;
    const rawPrice =
      scale.max - ((clampedY - TOP) * scale.range) / effectiveHeight;
    const price = snapToTickSize(rawPrice, tickSize);

    // Formatting: use slightly higher precision (+1 decimal) for inspection tools
    const step = Math.max(choosePriceStep(scale.range, timeframe), tickSize || 0);
    const hoverDec = Math.max(2, decimalsForStep(step) + 1);
    const label = formatPrice(price, hoverDec);

    // Layout and Measurement
    const padding = 6;
    priceOverlayContext.font = "13px 'JetBrains Mono', 'Fira Code', monospace";
    const hoverChipWidth = Math.max(
      52, // Minimum width for visual consistency
      Math.ceil(priceOverlayContext.measureText(label).width) + padding * 2
    );
    const hoverChipHeight = 20;
    const hoverChipX = Math.floor((PRICE_SCALE_W - hoverChipWidth) / 2);
    const hoverChipY = clamp(
      clampedY - hoverChipHeight / 2,
      2,
      glHeight - hoverChipHeight - 2 // Avoid clipping near edges
    );

    // Draw Hover Chip: Theme-based background (unlike the solid color Live chip)
    priceOverlayContext.fillStyle = colors.chipBg;
    priceOverlayContext.strokeStyle = colors.chipBorder;
    priceOverlayContext.lineWidth = 1.5;
    roundRectPath(
      priceOverlayContext,
      hoverChipX,
      hoverChipY,
      hoverChipWidth,
      hoverChipHeight,
      6
    );
    priceOverlayContext.fill();
    priceOverlayContext.stroke();

    // Draw text with theme-contrasting color
    priceOverlayContext.fillStyle = colors.chipText;
    priceOverlayContext.textAlign = "center";
    priceOverlayContext.textBaseline = "middle";
    drawAxisLabelText(
      priceOverlayContext,
      label,
      hoverChipX + hoverChipWidth / 2,
      hoverChipY + hoverChipHeight / 2 + 0.5
    );
  }, [
    PRICE_SCALE_W,
    glHeight,
    mouseYInner,
    scale.min,
    scale.max,
    scale.range,
    mappedBars,
    upCol,
    downCol,
    scale.y,
    colors,
    latestActualBar,
    tickSize,
  ]);

  /**
   * Renders chart overlay effects (interactive layer).
   * Displays:
   * 1. Live price line: horizontal dashed line at current close price (always visible)
   * 2. Crosshair: vertical + horizontal dashed lines when hovering over bar
   * 3. Hover summary: panel showing OHLCV data and % change for hovered bar
   * 4. External order levels: entry, target, stop loss from selected order
   */
  useEffect(() => {
    const chartOverlayCanvas = chartOverlayRef.current; // Get chart overlay canvas
    if (!chartOverlayCanvas) return; // Guard: skip if not mounted

    // Prepare 2D context
    const chartOverlayContext = prepCanvas2D(
      chartOverlayCanvas,
      chartAreaW,
      glHeight
    );

    // Clear canvas (removes previous overlays)
    chartOverlayContext.clearRect(0, 0, chartAreaW, glHeight);

    // SECTION 1: Draw live price line (always visible if we have bars)
    // This dashed horizontal line tracks the current market price (last bar close),
    // giving users an instant visual reference for where the market is right now.
    if (mappedBars.length > 0) {
      let latestBar = latestActualBar;

      if (!latestBar) {
        for (let i = mappedBars.length - 1; i >= 0; i--) {
          const candidate = mappedBars[i];
          if (candidate && !candidate.isFuture) {
            latestBar = candidate;
            break;
          }
        }
        if (!latestBar) latestBar = mappedBars[mappedBars.length - 1];
      }
      const isLatestBarUp = latestBar.close >= latestBar.open;
      const liveColor = isLatestBarUp ? upCol : downCol; // Color matches current direction
      const liveY = scale.y(latestBar.close) - TOP;
      const roundedY = Math.round(liveY);

      // Only draw if line is within visible vertical bounds
      if (liveY >= 0 && liveY <= glHeight) {
        chartOverlayContext.strokeStyle = liveColor;
        chartOverlayContext.lineWidth = 0.5;
        chartOverlayContext.setLineDash([6, 4]); // 6px dash, 4px gap
        chartOverlayContext.beginPath();
        chartOverlayContext.moveTo(0, roundedY);
        chartOverlayContext.lineTo(chartAreaW, roundedY);
        chartOverlayContext.stroke();
        chartOverlayContext.setLineDash([]); // Reset line style
        chartOverlayContext.lineWidth = 1;
      }
    }

    // SECTION 2: Entry/Target/Stop selector overlays
    if (selectorActive) {
      const priceStep = choosePriceStep(scale.range, timeframe);
      const priceDecimals = Math.max(2, decimalsForStep(priceStep));

      const drawLevel = (label, levelData, color) => {
        const price = levelData?.price;
        if (price == null) return;
        const y = Math.round(scale.y(price) - TOP);
        if (y < 0 || y > glHeight) return;

        let startX = 0;
        let endX = chartAreaW;
        const barCount = mappedBars.length;

        if (barCount > 0) {
          const startIdx =
            levelData?.startIndex != null
              ? clamp(levelData.startIndex, 0, barCount - 1)
              : 0;
          const endIdx =
            levelData?.endIndex != null
              ? clamp(levelData.endIndex, 0, barCount - 1)
              : barCount - 1;

          startX = LEFT + visibleRange.xOff + startIdx * colStride;
          endX = LEFT + visibleRange.xOff + (endIdx + 1) * colStride;

          startX = Math.max(0, Math.min(chartAreaW, startX));
          endX = Math.max(0, Math.min(chartAreaW, endX));
        }

        // Dashed guide line between start and end
        chartOverlayContext.strokeStyle = color;
        chartOverlayContext.lineWidth = 1;
        chartOverlayContext.setLineDash([6, 4]);
        chartOverlayContext.beginPath();
        chartOverlayContext.moveTo(startX, y);
        chartOverlayContext.lineTo(endX, y);
        chartOverlayContext.stroke();
        chartOverlayContext.setLineDash([]);

        // Start/end markers
        if (levelData?.startIndex != null || levelData?.endIndex != null) {
          chartOverlayContext.fillStyle = color;
          const r = 4;
          if (levelData?.startIndex != null) {
            chartOverlayContext.beginPath();
            chartOverlayContext.arc(startX, y, r, 0, Math.PI * 2);
            chartOverlayContext.fill();
          }
          if (levelData?.endIndex != null) {
            chartOverlayContext.beginPath();
            chartOverlayContext.arc(endX, y, r, 0, Math.PI * 2);
            chartOverlayContext.fill();
          }
        }

        // Price chip clinging to the right edge
        const chipPadding = 8;
        const chipHeight = 22;
        const labelText = `${label}: ${formatPrice(price, priceDecimals)}`;
        chartOverlayContext.font = "13px 'JetBrains Mono', 'Fira Code', monospace";
        const chipWidth = Math.ceil(
          chartOverlayContext.measureText(labelText).width + chipPadding * 2
        );
        const chipX = chartAreaW - chipWidth - RIGHT;
        const chipY = clamp(y - chipHeight / 2, 2, glHeight - chipHeight - 2);
        chartOverlayContext.fillStyle = colors.panelBg;
        chartOverlayContext.strokeStyle = colors.selectorHelper;
        chartOverlayContext.lineWidth = 1;
        roundRectPath(
          chartOverlayContext,
          chipX,
          chipY,
          chipWidth,
          chipHeight,
          5
        );
        chartOverlayContext.fill();
        chartOverlayContext.stroke();
        chartOverlayContext.fillStyle = color;
        chartOverlayContext.textAlign = "left";
        chartOverlayContext.textBaseline = "middle";
        chartOverlayContext.fillText(
          labelText,
          chipX + chipPadding,
          chipY + chipHeight / 2 + 0.5
        );
      };

      // Preview the next placement under the cursor until the level is set
      if (selectorLevels[selectorStep]?.price == null && mouseYInner != null) {
        const effectiveHeight = glHeight - TOP - BOTTOM;
        const previewRawPrice =
          scale.max -
          ((clamp(mouseYInner, 0, glHeight) - TOP) * scale.range) /
            Math.max(effectiveHeight, 1);
        const previewPrice = snapToTickSize(previewRawPrice, tickSize);
        const barCount = timelineBars.length;
        let previewStart = null;
        let previewEnd = null;
        if (barCount > 0) {
          const anchor =
            hoverIndex != null
              ? clamp(hoverIndex, 0, barCount - 1)
              : Math.max(0, lastActualIndex >= 0 ? lastActualIndex : barCount - 1);
          const rawPreviewEnd = lastActualIndex >= 0 ? lastActualIndex : Math.max(anchor, barCount - 1);
          
          // Ensure previewStart never exceeds previewEnd
          previewStart = Math.min(anchor, rawPreviewEnd);
          previewEnd = rawPreviewEnd;
        }
        const previewLabel =
          selectorStep === "entry"
            ? "Entry (preview)"
            : selectorStep === "target"
            ? "Target (preview)"
            : "Stoploss (preview)";
        drawLevel(
          previewLabel,
          { price: previewPrice, startIndex: previewStart, endIndex: previewEnd },
          colors.selectorHelper
        );
      }

      drawLevel("Entry", selectorLevels.entry, colors.orderEntryColor);
      drawLevel("Target", selectorLevels.target, colors.orderTargetColor);
      drawLevel("Stoploss", selectorLevels.stop, colors.orderStopColor);

      // Draw risk-reward ratio indicator as a single vertical dotted line at end index
      if (selectorReady && selectorLevels.entry?.price != null && 
          selectorLevels.target?.price != null && selectorLevels.stop?.price != null &&
          selectorLevels.entry?.endIndex != null) {
        const entryPrice = selectorLevels.entry.price;
        const targetPrice = selectorLevels.target.price;
        const stopPrice = selectorLevels.stop.price;
        const endIndex = selectorLevels.entry.endIndex;
        
        const risk = Math.abs(entryPrice - stopPrice);
        const reward = Math.abs(targetPrice - entryPrice);
        
        if (risk > 0 && mappedBars.length > 0 && endIndex >= 0 && endIndex < mappedBars.length) {
          const ratio = reward / risk;
          
          const entryY = Math.round(scale.y(entryPrice) - TOP);
          const targetY = Math.round(scale.y(targetPrice) - TOP);
          
          // Calculate X position at end index (same method used for bar rendering)
          const endX = Math.round(LEFT + visibleRange.xOff + endIndex * colStride);
          
          // Only draw if both levels are in view and X is in bounds
          if (entryY >= 0 && entryY <= glHeight && targetY >= 0 && targetY <= glHeight &&
              endX >= LEFT && endX <= LEFT + chartAreaW) {
            const startY = Math.min(entryY, targetY);
            const endY = Math.max(entryY, targetY);
            const midY = (startY + endY) / 2;
            
            // Draw vertical dotted line
            chartOverlayContext.save();
            chartOverlayContext.strokeStyle = colors.selectorHelper;
            chartOverlayContext.lineWidth = 1.5;
            chartOverlayContext.globalAlpha = 0.6;
            chartOverlayContext.setLineDash([4, 4]);
            
            chartOverlayContext.beginPath();
            chartOverlayContext.moveTo(endX, startY);
            chartOverlayContext.lineTo(endX, endY);
            chartOverlayContext.stroke();
            
            // Draw ratio text in the middle of the line
            chartOverlayContext.setLineDash([]);
            chartOverlayContext.globalAlpha = 1.0;
            chartOverlayContext.font = '11px monospace';
            chartOverlayContext.textAlign = 'center';
            chartOverlayContext.textBaseline = 'middle';
            
            const ratioText = `${ratio.toFixed(1)}`;
            const textMetrics = chartOverlayContext.measureText(ratioText);
            const textWidth = textMetrics.width;
            const textHeight = 14;
            const padding = 3;
            
            // Draw background rectangle for text
            chartOverlayContext.fillStyle = colors.chipBg;
            chartOverlayContext.fillRect(
              endX - textWidth / 2 - padding,
              midY - textHeight / 2 - padding,
              textWidth + padding * 2,
              textHeight + padding * 2
            );
            
            // Draw border around text
            chartOverlayContext.strokeStyle = colors.selectorHelper;
            chartOverlayContext.lineWidth = 1;
            chartOverlayContext.strokeRect(
              endX - textWidth / 2 - padding,
              midY - textHeight / 2 - padding,
              textWidth + padding * 2,
              textHeight + padding * 2
            );
            
            // Draw the ratio text
            chartOverlayContext.fillStyle = colors.selectorHelper;
            chartOverlayContext.fillText(ratioText, endX, midY);
            
            chartOverlayContext.restore();
          }
        }
      }

    }

    // Draw external order levels if provided (always visible when order is selected)
    const externalMatchesInstrument =
      externalOrder &&
      externalOrder.instrument_id &&
      String(externalOrder.instrument_id) === String(selectedInstrumentId);

    const entryPrice = Number(externalOrder?.entry_price);
    const targetPrice = Number(externalOrder?.target_price);
    const stopPrice = Number(externalOrder?.stoploss_price);
    const hasPrices = [entryPrice, targetPrice, stopPrice].every((v) => Number.isFinite(v));

    if (externalMatchesInstrument && hasPrices) {
      const entryY = scale.y(entryPrice);
      const targetY = scale.y(targetPrice);
      const stopY = scale.y(stopPrice);
      const entryLineColor = colors.orderEntryColor || colors.entryColor;
      const targetLineColor = colors.orderTargetColor || colors.targetColor;
      const stopLineColor = colors.orderStopColor || colors.stopColor;

      const barCount = mappedBars.length;
      const latestBarTs = latestActualBar?.date ?? null;

      const toNumericTimestamp = (value) => {
        if (value == null) return null;
        const numeric = Number(value);
        if (Number.isFinite(numeric)) return numeric;
        const parsed = new Date(value).getTime();
        return Number.isFinite(parsed) ? parsed : null;
      };

      const roundToPreviousMinute = (ts) =>
        ts == null ? null : Math.floor(ts / 60000) * 60000;

      const parseStateTransitions = (transitions) => {
        if (!transitions) return [];
        try {
          const parsed =
            typeof transitions === "string"
              ? JSON.parse(transitions)
              : transitions;
          return Array.isArray(parsed) ? parsed : [];
        } catch (error) {
          return [];
        }
      };

      const stateTransitions = parseStateTransitions(
        externalOrder?.state_transitions
      );
      const normalizedTransitions = stateTransitions
        .map((item) => {
          const state = String(item?.state || "").toUpperCase();
          const timestamp = toNumericTimestamp(item?.timestamp);
          return state ? { state, timestamp } : null;
        })
        .filter(Boolean);

      const completedTsFromTransitions = normalizedTransitions
        .filter((item) => item.state === "COMPLETED" && item.timestamp != null)
        .reduce((maxTs, item) => Math.max(maxTs, item.timestamp), -Infinity);
      const completedTsCandidate =
        completedTsFromTransitions !== -Infinity
          ? completedTsFromTransitions
          : toNumericTimestamp(externalOrder?.completed_at) ??
            toNumericTimestamp(externalOrder?.updated_at);
      const completedTs = roundToPreviousMinute(completedTsCandidate);

      const hasCompleted =
        normalizedTransitions.some((item) => item.state === "COMPLETED") ||
        String(externalOrder?.state || "").toUpperCase() === "COMPLETED";
      const hasCancelled =
        normalizedTransitions.some((item) => item.state === "CANCELLED") ||
        String(externalOrder?.state || "").toUpperCase() === "CANCELLED";

      const resolveEffectiveEndTs = (originalEndTs) => {
        if (hasCompleted && completedTs != null) return completedTs;
        if (hasCancelled) return originalEndTs;
        return latestBarTs ?? originalEndTs;
      };

      const resolveRange = (startTs, endTs) => {
        if (!barCount) {
          return {
            startX: 0,
            endX: chartAreaW,
            hasStart: false,
            hasEnd: false,
          };
        }

        const startIdxFromTs = findIndexByTimestamp(startTs);
        const endIdxFromTs = findIndexByTimestamp(endTs);

        const startIdx = startIdxFromTs ?? null;
        const endIdx = endIdxFromTs ?? null;

        const hasStart = startIdx != null;
        const hasEnd = endIdx != null;

        const safeStartIdx = hasStart ? startIdx : 0;
        const safeEndIdx = hasEnd ? endIdx : barCount - 1;

        const startX = clamp(
          LEFT + visibleRange.xOff + safeStartIdx * colStride,
          0,
          chartAreaW
        );
        const endX = clamp(
          LEFT + visibleRange.xOff + (safeEndIdx + 1) * colStride,
          0,
          chartAreaW
        );

        return { startX, endX, hasStart, hasEnd };
      };

      const entryRange = resolveRange(
        externalOrder.entry_start_ts,
        resolveEffectiveEndTs(externalOrder.entry_end_ts)
      );
      const targetRange = resolveRange(
        externalOrder.target_start_ts,
        resolveEffectiveEndTs(externalOrder.target_end_ts)
      );
      const stopRange = resolveRange(
        externalOrder.stop_start_ts,
        resolveEffectiveEndTs(externalOrder.stop_end_ts)
      );

      const drawOrderLevel = (y, color, range) => {
        chartOverlayContext.strokeStyle = color;
        chartOverlayContext.lineWidth = 1.5;
        chartOverlayContext.setLineDash([4, 4]);
        chartOverlayContext.beginPath();
        chartOverlayContext.moveTo(range.startX, y);
        chartOverlayContext.lineTo(range.endX, y);
        chartOverlayContext.stroke();
        chartOverlayContext.setLineDash([]);

        // Markers when range is known
        if (range.hasStart || range.hasEnd) {
          chartOverlayContext.fillStyle = color;
          const r = 4;
          if (range.hasStart) {
            chartOverlayContext.beginPath();
            chartOverlayContext.arc(range.startX, y, r, 0, Math.PI * 2);
            chartOverlayContext.fill();
          }
          if (range.hasEnd) {
            chartOverlayContext.beginPath();
            chartOverlayContext.arc(range.endX, y, r, 0, Math.PI * 2);
            chartOverlayContext.fill();
          }
        }
      };

      drawOrderLevel(entryY, entryLineColor, entryRange);
      drawOrderLevel(targetY, targetLineColor, targetRange);
      drawOrderLevel(stopY, stopLineColor, stopRange);

      // Reset line dash for labels
      chartOverlayContext.setLineDash([]);
      chartOverlayContext.font = "11px monospace";
      chartOverlayContext.textBaseline = "middle";

      // Entry label - positioned on right edge
      const entryLabel = `Entry: ${entryPrice.toFixed(2)}`;
      const entryLabelW = chartOverlayContext.measureText(entryLabel).width;
      const entryLabelX = chartAreaW - entryLabelW - 16;
      chartOverlayContext.fillStyle = entryLineColor;
      chartOverlayContext.fillRect(entryLabelX - 4, entryY - 10, entryLabelW + 8, 20);
      chartOverlayContext.fillStyle = "#ffffff";
      chartOverlayContext.fillText(entryLabel, entryLabelX, entryY);

      // Target label - positioned on right edge
      const targetLabel = `Target: ${targetPrice.toFixed(2)}`;
      const targetLabelW = chartOverlayContext.measureText(targetLabel).width;
      const targetLabelX = chartAreaW - targetLabelW - 16;
      chartOverlayContext.fillStyle = targetLineColor;
      chartOverlayContext.fillRect(targetLabelX - 4, targetY - 10, targetLabelW + 8, 20);
      chartOverlayContext.fillStyle = "#ffffff";
      chartOverlayContext.fillText(targetLabel, targetLabelX, targetY);

      // Stop label - positioned on right edge
      const stopLabel = `Stoploss: ${stopPrice.toFixed(2)}`;
      const stopLabelW = chartOverlayContext.measureText(stopLabel).width;
      const stopLabelX = chartAreaW - stopLabelW - 16;
      chartOverlayContext.fillStyle = stopLineColor;
      chartOverlayContext.fillRect(stopLabelX - 4, stopY - 10, stopLabelW + 8, 20);
      chartOverlayContext.fillStyle = "#ffffff";
      chartOverlayContext.fillText(stopLabel, stopLabelX, stopY);

      // Draw order info panel
      chartOverlayContext.font = "12px monospace";
      chartOverlayContext.textBaseline = "top";
      const orderInfoLines = [
        `Order: ${externalOrder.symbol}`,
        `Side: ${externalOrder.side}`,
        `Quantity: ${externalOrder.qty}`,
        `State: ${externalOrder.state}`,
      ];
      const orderPanelW = 200;
      const orderPanelH = orderInfoLines.length * 18 + 12;
      // Move panel to the left so price chips on the right remain visible
      const orderPanelX = 8;
      const orderPanelY = 36;

      chartOverlayContext.fillStyle = colors.overlayBg;
      chartOverlayContext.fillRect(orderPanelX, orderPanelY, orderPanelW, orderPanelH);
      chartOverlayContext.strokeStyle = colors.overlayBorder;
      chartOverlayContext.lineWidth = 1;
      chartOverlayContext.strokeRect(orderPanelX, orderPanelY, orderPanelW, orderPanelH);

      chartOverlayContext.fillStyle = colors.textColor;
      orderInfoLines.forEach((line, i) => {
        chartOverlayContext.fillText(line, orderPanelX + 8, orderPanelY + 8 + i * 18);
      });

      // Draw risk-reward ratio indicator for external order
      const risk = Math.abs(entryPrice - stopPrice);
      const reward = Math.abs(targetPrice - entryPrice);
      
      if (risk > 0 && entryRange.hasEnd) {
        const ratio = reward / risk;
        
        const entryYInner = Math.round(entryY - TOP);
        const targetYInner = Math.round(targetY - TOP);
        
        // Use entry end X position for the ratio line
        const ratioX = entryRange.endX;
        
        // Only draw if both levels are in view and X is in bounds
        if (entryYInner >= 0 && entryYInner <= glHeight && 
            targetYInner >= 0 && targetYInner <= glHeight &&
            ratioX >= LEFT && ratioX <= LEFT + chartAreaW) {
          const startY = Math.min(entryYInner, targetYInner);
          const endY = Math.max(entryYInner, targetYInner);
          const midY = (startY + endY) / 2;
          
          // Draw vertical dotted line
          chartOverlayContext.save();
          chartOverlayContext.strokeStyle = colors.selectorHelper;
          chartOverlayContext.lineWidth = 1.5;
          chartOverlayContext.globalAlpha = 0.6;
          chartOverlayContext.setLineDash([4, 4]);
          
          chartOverlayContext.beginPath();
          chartOverlayContext.moveTo(ratioX, startY);
          chartOverlayContext.lineTo(ratioX, endY);
          chartOverlayContext.stroke();
          
          // Draw ratio text in the middle of the line
          chartOverlayContext.setLineDash([]);
          chartOverlayContext.globalAlpha = 1.0;
          chartOverlayContext.font = '11px monospace';
          chartOverlayContext.textAlign = 'center';
          chartOverlayContext.textBaseline = 'middle';
          
          const ratioText = `${ratio.toFixed(1)}`;
          const textMetrics = chartOverlayContext.measureText(ratioText);
          const textWidth = textMetrics.width;
          const textHeight = 14;
          const padding = 3;
          
          // Draw background rectangle for text
          chartOverlayContext.fillStyle = colors.chipBg;
          chartOverlayContext.fillRect(
            ratioX - textWidth / 2 - padding,
            midY - textHeight / 2 - padding,
            textWidth + padding * 2,
            textHeight + padding * 2
          );
          
          // Draw border around text
          chartOverlayContext.strokeStyle = colors.selectorHelper;
          chartOverlayContext.lineWidth = 1;
          chartOverlayContext.strokeRect(
            ratioX - textWidth / 2 - padding,
            midY - textHeight / 2 - padding,
            textWidth + padding * 2,
            textHeight + padding * 2
          );
          
          // Draw the ratio text
          chartOverlayContext.fillStyle = colors.selectorHelper;
          chartOverlayContext.fillText(ratioText, ratioX, midY);
          
          chartOverlayContext.restore();
        }
      }
    }

    // Guard: Exit if no active interaction (hover) or no data
    if (hoverIndex == null || mappedBars.length === 0) return;

    // Draw Crosshairs only when crosshair mode is enabled
    if (crosshairEnabled) {
      // SECTION 2: Draw Crosshairs
      // Calculate snapped X position based on hovering bar index
      const barCount = mappedBars.length;
      const clampedHoverIndex = clamp(hoverIndex, 0, barCount - 1);
      const hoverXCenter =
        LEFT + visibleRange.xOff + clampedHoverIndex * colStride + colStride / 2;

      // Skip if hovered bar is effectively off-screen
      if (hoverXCenter >= LEFT && hoverXCenter <= chartAreaW - RIGHT) {
        // Draw Horizontal Crosshair (follows precise mouse Y)
        if (mouseYInner != null) {
          const effectiveHeight = glHeight - TOP - BOTTOM;
          const rawPrice =
            scale.max -
            ((clamp(mouseYInner, 0, glHeight) - TOP) * scale.range) /
              Math.max(effectiveHeight, 1);
          const snappedPrice = snapToTickSize(rawPrice, tickSize);
          const yLine = clamp(scale.y(snappedPrice) - TOP, 0, glHeight);
          chartOverlayContext.strokeStyle = colors.crosshairColor;
          chartOverlayContext.setLineDash([4, 4]); // Tighter dash for crosshair
          chartOverlayContext.beginPath();
          chartOverlayContext.moveTo(0, yLine);
          chartOverlayContext.lineTo(chartAreaW, yLine);
          chartOverlayContext.stroke();
          chartOverlayContext.setLineDash([]);
        }

        // Draw Vertical Crosshair (snaps to discrete bar column)
        chartOverlayContext.strokeStyle = colors.crosshairColor;
        chartOverlayContext.setLineDash([4, 4]);
        chartOverlayContext.beginPath();
        chartOverlayContext.moveTo(Math.round(hoverXCenter), 0);
        chartOverlayContext.lineTo(Math.round(hoverXCenter), glHeight);
        chartOverlayContext.stroke();
        chartOverlayContext.setLineDash([]);
      }
    }

    // SECTION 3: Draw OHLCV Info Panel
    // Displays Open, High, Low, Close, Volume, and % Change for the hovered bar.
    // Positioned at the top-left of the chart area.
    const clampedHoverIndex = clamp(hoverIndex, 0, mappedBars.length - 1);
    const hoveredBar = mappedBars[clampedHoverIndex];
    const isFutureSlot = hoveredBar?.isFuture;
    let hoverColor = colors.textColor;

    let hoverSummary = "";
    if (hoveredBar && isFutureSlot) {
      // Show the most recent actual bar stats when hovering over future slots
      const latestReal =
        latestActualBar ||
        (() => {
          for (let i = mappedBars.length - 1; i >= 0; i--) {
            const candidate = mappedBars[i];
            if (candidate && !candidate.isFuture) return candidate;
          }
          return null;
        })();

      if (latestReal) {
        const isUp = latestReal.close >= latestReal.open;
        hoverColor = isUp ? upCol : downCol;
        const percentageChange =
          latestReal.open !== 0
            ? ((latestReal.close - latestReal.open) / latestReal.open) * 100
            : 0;

        hoverSummary = `O:${formatPrice(latestReal.open)}  H:${formatPrice(
          latestReal.high
        )}  L:${formatPrice(latestReal.low)}  C:${formatPrice(
          latestReal.close
        )}  Δ ${percentageChange >= 0 ? "+" : ""}${percentageChange.toFixed(
          2
        )}%  V:${formatVol(latestReal.volume)}`;
      } else {
        // Fallback if no real bars exist
        const timeLabel = formatTimeLabel(
          hoveredBar.date,
          timeframe,
          barWidth,
          is_intraday
        );
        hoverSummary = `Future slot ${timeLabel}`;
      }
    } else if (hoveredBar) {
      const isHoveredUp = hoveredBar.close >= hoveredBar.open;
      hoverColor = isHoveredUp ? upCol : downCol; // Text color matches bar direction

      const percentageChange =
        hoveredBar.open !== 0
          ? ((hoveredBar.close - hoveredBar.open) / hoveredBar.open) * 100
          : 0;

      // Construct Summary String
      hoverSummary = `O:${formatPrice(hoveredBar.open)}  H:${formatPrice(
        hoveredBar.high
      )}  L:${formatPrice(hoveredBar.low)}  C:${formatPrice(
        hoveredBar.close
      )}  Δ ${percentageChange >= 0 ? "+" : ""}${percentageChange.toFixed(
        2
      )}%  V:${formatVol(hoveredBar.volume)}`;
    }

    // Style and measure text to size the panel dynamically
    const padding = 8;
    chartOverlayContext.font = "14px 'JetBrains Mono', 'Fira Code', monospace";
    const summaryWidth = Math.ceil(
      chartOverlayContext.measureText(hoverSummary).width
    );
    const panelWidth = summaryWidth + padding * 2;
    const panelHeight = 24;

    // Position panel at top-left of chart area
    const panelX = 2; // Fixed left margin
    const panelY = 2; // Fixed top margin

    // Draw panel background with rounded corners
    chartOverlayContext.fillStyle = colors.panelBg;
    chartOverlayContext.strokeStyle = colors.panelBorder;
    chartOverlayContext.lineWidth = 1;
    roundRectPath(
      chartOverlayContext,
      panelX,
      panelY,
      panelWidth,
      panelHeight,
      6 // Corner radius
    );
    chartOverlayContext.fill();
    chartOverlayContext.stroke();

    // Draw summary text inside the panel
    // Text color matches the bar direction (green/red) for visual correlation
    chartOverlayContext.fillStyle = hoverColor;
    chartOverlayContext.textAlign = "left";
    chartOverlayContext.textBaseline = "middle";
    chartOverlayContext.fillText(
      hoverSummary,
      panelX + padding,
      panelY + panelHeight / 2 + 0.5 // Subpixel vertical centering
    );
  }, [
    chartAreaW,
    glHeight,
    hoverIndex,
    mappedBars,
    visibleRange.xOff,
    colStride,
    mouseYInner,
    LEFT,
    RIGHT,
    upCol,
    downCol,
    scale.y,
    scale.min,
    scale.max,
    scale.range,
    colors,
    latestActualBar,
    timeframe,
    barWidth,
    is_intraday,
    crosshairEnabled,
    selectorActive,
    selectorLevels.entry,
    selectorLevels.target,
    selectorLevels.stop,
    selectorStep,
    selectorReady,
    selectorValidation.direction,
    tickSize,
    externalOrder,
    findIndexByTimestamp,
  ]);

  /**
   * Applies inertia to panning animation.
   * When user releases the mouse after dragging, the chart continues scrolling with decelerating velocity.
   * Creates a smooth, natural feel similar to mobile apps.
   *
   * Also recenters the chart when initial data loads (barWidth === 12 is default).
   */
  useEffect(() => {
    // Recenter chart when bars first load and width is at default (12px)
    // This ensures the chart starts right-aligned instead of left-aligned
    if (mappedBars.length > 0 && chartAreaW > 0 && barWidth === 12) {
      const lastRealWidth =
        lastActualIndex >= 0 ? (lastActualIndex + 1) * colStride : 0;
      const totalWidth = lastRealWidth || mappedBars.length * colStride;
      // Right-align to last real bar; ignore placeholders for initial view
      setOffsetX(Math.min(0, chartAreaW - LEFT - RIGHT - totalWidth));
    }

    // Setup inertia animation loop
    let raf = 0; // RequestAnimationFrame ID for cancellation

    // Animation tick function: applies velocity each frame
    const tick = () => {
      // Only continue if velocity is significant (> 0.1 pixels/frame)
      if (Math.abs(velRef.current) > 0.1) {
        // Update pan offset by current velocity
        setOffsetX((currentOffset) =>
          clamp(
            currentOffset + velRef.current, // Apply velocity
            // Clamp to reasonable bounds (prevent over-panning)
            minOffsetLimit - colStride * 2, // Rightmost boundary with buffer
            colStride * 2 // Right boundary with buffer
          )
        );

        // Decelerate velocity by 12% each frame (88% of previous)
        // Creates natural exponential decay animation
        velRef.current *= 0.88;

        // Schedule next frame
        raf = requestAnimationFrame(tick);
      }
    };

    // Start animation loop
    raf = requestAnimationFrame(tick);

    // Cleanup: cancel animation on unmount or dependency change
    return () => cancelAnimationFrame(raf);
  }, [
    mappedBars.length,
    colStride,
    chartAreaW,
    LEFT,
    RIGHT,
    barWidth,
    lastActualIndex,
    minOffsetLimit,
  ]);

  /**
   * Pointer interaction handler for mouse and touch events.
   * Implements:
   * - Hover detection: identifies which bar is under cursor
   * - Drag panning: moves chart left/right with pointer drag
   * - Wheel zoom: vertical scroll zooms in/out, horizontal scroll pans
   * - Inertia: applies velocity to continue panning after release
   *
   * Respects modal states (search/settings open) to prevent interactions during modals.
   */
  useEffect(() => {
    const target = containerRef.current; // Chart container element
    if (!target) return; // Guard: skip if not mounted

    // State tracking for drag operations
    let isDragging = false; // Is pointer currently being dragged
    let lastPointerPrimary = 0; // Last pointer position along time-axis drag direction

    const getVirtualPointer = (pointerX, pointerY) => {
      const plotLeft = TIME_SCALE_H;
      const plotTop = PRICE_SCALE_W;
      const plotRight = plotLeft + glHeight;
      const plotBottom = plotTop + chartAreaW;

      const insidePlot =
        pointerX >= plotLeft &&
        pointerX <= plotRight &&
        pointerY >= plotTop &&
        pointerY <= plotBottom;

      if (!insidePlot) {
        return { insidePlot: false, virtualX: null, virtualY: null };
      }

      const localX = pointerX - plotLeft;
      const localY = pointerY - plotTop;

      return {
        insidePlot: true,
        virtualX: clamp(chartAreaW - localY, 0, chartAreaW),
        virtualY: clamp(localX, 0, glHeight),
      };
    };

    /**
     * Helper: Converts pointer X position to nearest bar index.
     * Maps pixel position back to data column with proper boundary detection.
     * Only activates within the drawable chart area (between LEFT and chartAreaW-RIGHT).
     * @param {number} pointerX - Mouse X position in viewport
     * @returns {number|null} - Bar index or null if outside chart
     */
    const calculateHoverIndex = (virtualX) => {
      const barCount = mappedBars.length;
      if (!barCount) return null; // No data

      // Only detect hover within the drawable chart area (respecting LEFT/RIGHT padding)
      const chartStart = LEFT;
      const chartEnd = chartAreaW - RIGHT;
      
      if (virtualX == null || virtualX < chartStart || virtualX >= chartEnd) {
        return null; // Outside drawable bounds
      }

      // Calculate which bar is at this X position
      // Formula: (pointerX - LEFT - offsetX) / colStride
      // This maps screen pixel → bar index
      const relativeX = virtualX - chartStart - xOffRef.current;
      const columnIndex = Math.floor(relativeX / colStride);
      
      // Clamp to valid bar range
      return clamp(columnIndex, 0, barCount - 1);
    };

    // Convert a container-relative Y to price using current scale
    const chartPriceFromPointer = (virtualY) => {
      if (virtualY == null) return null;
      const clampedY = clamp(virtualY, 0, glHeight);
      const effectiveHeight = glHeight - TOP - BOTTOM;
      const activeScale = scaleRef.current;
      return (
        activeScale.max -
        ((clampedY - TOP) * activeScale.range) / Math.max(effectiveHeight, 1)
      );
    };

    // Hit test for selector lines to enable drag once placed
    const selectorHitTest = (virtualY) => {
      if (!selectorActive) return null;
      if (virtualY == null || virtualY < 0 || virtualY > glHeight) return null;
      const tolerancePx = 10;
      let closest = null;
      let closestDist = Infinity;
      const candidates = [
        ["entry", selectorLevels.entry],
        ["target", selectorLevels.target],
        ["stop", selectorLevels.stop],
      ];
      for (const [key, value] of candidates) {
        if (value?.price == null) continue;
        const activeScale = scaleRef.current;
        const y = Math.round(activeScale.y(value.price) - TOP);
        const dist = Math.abs(y - virtualY);
        if (dist <= tolerancePx && dist < closestDist) {
          closest = key;
          closestDist = dist;
        }
      }
      return closest;
    };

    /**
     * Pointer enter handler: mark cursor inside container.
     */
    const handlePointerEnter = () =>
      setMouse((state) => ({ ...state, inside: true }));

    /**
     * Pointer leave handler: clear hover state when leaving container.
     * Resets all interactive elements (crosshair, tooltips, drag velocity).
     */
    const handlePointerLeave = () => {
      setMouse({ x: null, y: null, inside: false }); // Clear position
      setHoverIndex(null); // Clear bar hover
      setMouseYInner(null); // Clear price hover
      velRef.current = 0; // Stop any inertia animation
      selectorDragRef.current = null;
    };

    /**
     * Pointer move handler: tracks cursor position for crosshairs and tooltips.
     * Calculates two distinct coordinates:
     * 1. X-axis: mapped to specific bar index (for time scale)
     * 2. Y-axis: mapped to price value (for price scale)
     */
    const handlePointerMove = (event) => {
      // Disable interaction if modal overlays are active
      if (searchOpen || settingsOpen) return;

      // Calculate pointer position relative to chart container
      const boundingRect = target.getBoundingClientRect();
      const pointerX = event.clientX - boundingRect.left;
      const pointerY = event.clientY - boundingRect.top;

      // Ignore events in the bottom bar area (control strip)
      if (pointerY > boundingRect.height - BOTTOM_BAR_H) return;

      const virtualPointer = getVirtualPointer(pointerX, pointerY);

      setMouse({ x: pointerX, y: pointerY, inside: true });

      // Dragging an existing selector level: update its price continuously
      if (selectorDragRef.current) {
        const rawPrice = chartPriceFromPointer(virtualPointer.virtualY);
        if (rawPrice == null) return;
        const updatedPrice = snapToTickSize(rawPrice, tickSize);
        setSelectorLevels((prev) => {
          const current = prev[selectorDragRef.current] || {};
          const next = {
            ...prev,
            [selectorDragRef.current]: { ...current, price: updatedPrice },
          };
          const validation = validateSelectorLevels(next);
          if (validation.error) {
            setSelectorError(validation.error);
            return prev;
          }
          setSelectorError("");
          return next;
        });
        if (virtualPointer.virtualY != null) {
          setMouseYInner(virtualPointer.virtualY);
        }
        return;
      }

      // Only update price crosshair if cursor is within the main chart grid
      if (virtualPointer.virtualY != null) setMouseYInner(virtualPointer.virtualY);
      else setMouseYInner(null);

      // Map X pixel position to the specific bar index
      setHoverIndex(calculateHoverIndex(virtualPointer.virtualX));
    };

    /**
     * Pointer down handler: initiates drag operations.
     */
    const handlePointerDown = (event) => {
      if (searchOpen || settingsOpen) return;
      const boundingRect = target.getBoundingClientRect();
      const pointerX = event.clientX - boundingRect.left;
      const pointerY = event.clientY - boundingRect.top;

      // Ignore clicks in the bottom bar area
      if (pointerY > boundingRect.height - BOTTOM_BAR_H) return;

      const virtualPointer = getVirtualPointer(pointerX, pointerY);

      // Selector mode: capture level clicks instead of panning
      if (selectorActive) {
        const hitLevel = selectorHitTest(virtualPointer.virtualY);
        if (hitLevel) {
          try {
            target.setPointerCapture?.(event.pointerId ?? 0);
          } catch {}
          selectorDragRef.current = hitLevel;
          if (virtualPointer.virtualY != null) {
            setMouseYInner(virtualPointer.virtualY);
          }
          return;
        }
        // When all levels are set, ignore extra clicks unless user drags or clears/executes
        if (selectorReady) return;
        if (!virtualPointer.insidePlot) return;
        const rawPrice = chartPriceFromPointer(virtualPointer.virtualY);
        if (rawPrice == null) return;
        const priceAtPointer = snapToTickSize(rawPrice, tickSize);
        const barIndexAtPointer = calculateHoverIndex(virtualPointer.virtualX);
        setSelectorLevels((prev) => {
          const rawStartIndex =
            barIndexAtPointer != null
              ? barIndexAtPointer
              : lastActualIndex >= 0
              ? lastActualIndex
              : timelineBars.length - 1;
          const rawEndIndex =
            lastActualIndex >= 0 ? lastActualIndex : rawStartIndex;
          
          // Ensure startIndex never exceeds endIndex
          const resolvedStartIndex = Math.min(rawStartIndex, rawEndIndex);
          const resolvedEndIndex = rawEndIndex;
          
          const next = {
            ...prev,
            [selectorStep]: {
              price: priceAtPointer,
              startIndex: resolvedStartIndex,
              endIndex: resolvedEndIndex,
            },
            ...(selectorStep === "entry"
              ? {
                  target: { price: null, startIndex: null, endIndex: null },
                  stop: { price: null, startIndex: null, endIndex: null },
                }
              : {}),
          };

          const validation = validateSelectorLevels(next);
          if (validation.error) {
            setSelectorError(validation.error);
            return prev;
          }

          setSelectorError("");
          setSelectorStep((step) =>
            step === "entry" ? "stop" : step === "stop" ? "target" : "entry"
          );
          return next;
        });
        return;
      }

      // Capture pointer to track drag even if cursor leaves container
      try {
        target.setPointerCapture?.(event.pointerId ?? 0);
      } catch {}

      isDragging = true;
      lastPointerPrimary = event.clientY;
      velRef.current = 0; // Reset inertia on new interaction
    };

    const handlePointerUp = () => {
      isDragging = false;
      selectorDragRef.current = null;
    };

    /**
     * Pointer drag handler: pans the chart contents.
     * Adjusts `offsetX` (visible window) based on horizontal movement.
     */
    const handlePointerDrag = (event) => {
      if (selectorDragRef.current) {
        const boundingRect = target.getBoundingClientRect();
        const pointerX = event.clientX - boundingRect.left;
        const pointerY = event.clientY - boundingRect.top;
        const chartHeight = boundingRect.height - BOTTOM_BAR_H;
        if (pointerY > chartHeight) return;
        const virtualPointer = getVirtualPointer(pointerX, pointerY);
        const rawPrice = chartPriceFromPointer(virtualPointer.virtualY);
        if (rawPrice == null) return;
        const updatedPrice = snapToTickSize(rawPrice, tickSize);
        setSelectorLevels((prev) => {
          const current = prev[selectorDragRef.current] || {};
          const next = {
            ...prev,
            [selectorDragRef.current]: { ...current, price: updatedPrice },
          };
          const validation = validateSelectorLevels(next);
          if (validation.error) {
            setSelectorError(validation.error);
            return prev;
          }
          setSelectorError("");
          return next;
        });
        if (virtualPointer.virtualY != null) {
          setMouseYInner(virtualPointer.virtualY);
        }
        return;
      }

      if (searchOpen || settingsOpen || !isDragging) return;

      const deltaY = event.clientY - lastPointerPrimary;
      const deltaAlongTime = -deltaY;
      lastPointerPrimary = event.clientY;
      velRef.current = deltaAlongTime; // Record velocity for inertia release

      // Update scroll offset with bounds checking
      setOffsetX((currentOffset) =>
        clamp(
          currentOffset + deltaAlongTime,
          // Rightmost scroll limit (keep at least one real bar visible)
          minOffsetLimit - colStride * 2,
          colStride * 2 // Allow small overscroll buffer at start
        )
      );

      // Update hover index during drag for immediate feedback
      const latestMouse = mouseRef.current;
      const latestVirtualPointer = getVirtualPointer(latestMouse.x, latestMouse.y);
      setHoverIndex(
        latestMouse.inside
          ? calculateHoverIndex(latestVirtualPointer.virtualX)
          : null
      );
    };

    /**
     * Wheel handler: implements both Zoom and Pan.
     * - Ctrl/Command + Scroll OR Pinch: Zooms in/out
     * - Regular Scroll: Pans left/right
     */
    const handleWheel = (event) => {
      if (searchOpen || settingsOpen) return;

      // Bounds check against bottom bar
      const boundingRect = target.getBoundingClientRect();
      const pointerY = event.clientY - boundingRect.top;
      const chartHeight = measuredHeight - BOTTOM_BAR_H;
      if (pointerY > chartHeight) return;

      event.preventDefault(); // Prevent browser page scrolling
      const { x: deltaXRaw, y: deltaYRaw } = normalizeWheel(event);
      const pointerX = event.clientX - boundingRect.left;
      const virtualPointer = getVirtualPointer(pointerX, pointerY);

      // Update price crosshair if valid
      if (virtualPointer.virtualY != null)
        setMouseYInner(virtualPointer.virtualY);
      else setMouseYInner(null);

      // ZOOM LOGIC: Triggered by vertical scroll dominance or Ctrl key (pinch gesture)
      if (Math.abs(deltaYRaw) > Math.abs(deltaXRaw) || event.ctrlKey) {
        const zoomDelta = event.ctrlKey ? deltaXRaw : deltaYRaw;

        // Calculate zoom multiplier (exponential for smoothness)
        const zoomFactor = Math.exp(-zoomDelta * 0.002);

        // Compute new bar width (clamped 1px to 100px)
        const newBarWidth = clamp(barWidth * zoomFactor, 1, 100);

        // Update state with a callback to ensure coordinate sync
        setBarWidth((previousWidth) => {
          // Calculate spacing metrics
          const oldGap = computeGap(previousWidth);
          const newGap = computeGap(newBarWidth);
          const oldStride = previousWidth + oldGap;
          const newStride = newBarWidth + newGap;

          // ADJUST OFFSET: "Zoom to Cursor"
          // We want the bar under the cursor to remain under the cursor after zoom.
          setOffsetX((currentOffset) => {
            // 1. Find which specific bar is currently under the mouse
            const barAtMouseRaw =
              ((virtualPointer.virtualX ?? LEFT) - (LEFT + currentOffset)) /
              oldStride;
            const barAtMouse = clamp(
              barAtMouseRaw,
              0,
              Math.max(0, timelineBars.length - 1)
            );

            // 2. Calculate what the new offset should in order to keep that bar at pointerX
            const newOffset =
              (virtualPointer.virtualX ?? LEFT) - LEFT - barAtMouse * newStride;

            // 3. Clamp the new offset to valid scroll bounds (respect last actual bar)
            const futureMin = Math.min(
              0,
              chartAreaW - LEFT - RIGHT - timelineBars.length * newStride
            );
            const anchorIndex =
              lastActualIndex >= 0 ? Math.max(0, lastActualIndex - 2) : 0;
            const keepAnchorVisible = -anchorIndex * newStride;
            const minOff = Math.max(futureMin, keepAnchorVisible);

            return clamp(newOffset, minOff - newStride * 2, newStride * 2);
          });
          return newBarWidth;
        });
        velRef.current = 0; // Kill inertia during zoom
      }
      // PAN LOGIC: Horizontal scroll or touchpad swipe
      else {
        const panInput =
          Math.abs(deltaYRaw) >= Math.abs(deltaXRaw) ? deltaYRaw : deltaXRaw;
        setOffsetX((currentOffset) => {
          const next = currentOffset - panInput * 0.7; // 0.7 dampening factor
          return clamp(next, minOffsetLimit - colStride * 2, colStride * 2);
        });
        velRef.current = -panInput * 0.7; // Update inertia
      }

      setHoverIndex(calculateHoverIndex(virtualPointer.virtualX));
    };

    // Attach native event listeners
    // Note: 'wheel' must be non-passive to allow preventDefault()
    target.addEventListener("mouseenter", handlePointerEnter);
    target.addEventListener("mouseleave", handlePointerLeave);
    target.addEventListener("pointermove", handlePointerMove);
    target.addEventListener("pointerdown", handlePointerDown);
    window.addEventListener("pointerup", handlePointerUp);
    window.addEventListener("pointermove", handlePointerDrag, { passive: false });
    target.addEventListener("wheel", handleWheel, { passive: false });
    return () => {
      target.removeEventListener("mouseenter", handlePointerEnter);
      target.removeEventListener("mouseleave", handlePointerLeave);
      target.removeEventListener("pointermove", handlePointerMove);
      target.removeEventListener("pointerdown", handlePointerDown);
      window.removeEventListener("pointerup", handlePointerUp);
      window.removeEventListener("pointermove", handlePointerDrag);
      target.removeEventListener("wheel", handleWheel);
    };
  }, [
    barWidth,
    colStride,
    TIME_SCALE_H,
    chartAreaW,
    glHeight,
    mappedBars.length,
    LEFT,
    RIGHT,
    searchOpen,
    settingsOpen,
    measuredHeight,
    minOffsetLimit,
    timelineBars.length,
    lastActualIndex,
    selectorActive,
    selectorStep,
    selectorLevels.entry,
    selectorLevels.target,
    selectorLevels.stop,
    tickSize,
  ]);

  /**
   * Instrument Search Effect.
   * Handles real-time search queries with debouncing to prevent excessive API calls.
   *
   * logic:
   * 1. Validates input length (min 3 chars).
   * 2. Waits 250ms after last keystroke (debounce).
   * 3. Calls search API with current filters (exchange, segment).
   * 4. Updates results list or handles errors.
   */
  useEffect(() => {
    // Case 1: Empty query - clear everything immediately
    if (!searchQuery.trim()) {
      setSearchLoading(false);
      setSearchResults([]);
      return;
    }

    // Case 2: Query too short - clear results, don't search yet
    const trimmed = searchQuery.trim();
    if (trimmed.length < 3) {
      setSearchLoading(false);
      setSearchResults([]);
      return;
    }

    // Case 3: Valid query - execute search after delay
    const handle = setTimeout(async () => {
      setSearchLoading(true);
      try {
        // Execute API search request
        const res = await searchInstrument(
          searchExchange,
          searchSegment,
          trimmed,
          true
        );
        // Safety check: ensure response is an array before setting state
        setSearchResults(Array.isArray(res) ? res : []);
      } catch (err) {
        console.error("search error:", err);
        setSearchResults([]); // Fallback to empty list on error
      } finally {
        setSearchLoading(false);
      }
    }, 250); // 250ms debounce window

    // Cleanup: Cancel pending search if user types again before delay finishes
    return () => clearTimeout(handle);
  }, [searchQuery, searchExchange, searchSegment]);

  /**
   * Shared JSX fragment for the multi-layer canvas architecture.
   *
   * Layout (back to front):
   * 1. Time scale area (top): timeBaseRef (grid) + timeLiveRef (hover tooltip)
   * 2. Chart area (middle): chartGLRef (WebGL bars) + chartLiveRef (overlays)
   * 3. Price scale area (right): priceScaleBaseRef (grid) + priceScaleLiveRef (hover chips)
   *
   * All canvases are absolutely positioned and sized based on layout constants.
   * Empty state message when no data is loaded.
   */
  const rotatedTransform = `translateY(${chartAreaW}px) rotate(-90deg)`;
  const rotatedPriceTransform = `translateY(${PRICE_SCALE_W}px) rotate(-90deg)`;

  const ChartArea = (
    <>
      <div
        className="absolute left-0"
        style={{ top: PRICE_SCALE_W, width: TIME_SCALE_H, height: chartAreaW }}
      >
        <div
          className="absolute left-0 top-0"
          style={{
            width: chartAreaW,
            height: TIME_SCALE_H,
            transform: rotatedTransform,
            transformOrigin: "top left",
          }}
        >
          <canvas ref={timeBaseRef} className="absolute inset-0" />
          <canvas
            ref={timeOverlayRef}
            className="pointer-events-none absolute inset-0"
          />
        </div>
      </div>

      <div
        className="absolute left-0 top-0 flex items-center justify-center"
        style={{
          width: TIME_SCALE_H,
          height: PRICE_SCALE_W,
          backgroundColor: colors.chipBg,
          borderBottom: `1px solid ${colors.chipBorder}`,
          borderRight: `1px solid ${colors.chipBorder}`,
        }}
      >
        <span
          style={{
            color: colors.controlText,
            fontSize: "12px",
            fontWeight: "600",
            letterSpacing: "0.5px",
          }}
        >
          Live
        </span>
      </div>

      <div
        className="absolute"
        style={{
          left: TIME_SCALE_H,
          top: PRICE_SCALE_W,
          width: glHeight,
          height: chartAreaW,
        }}
      >
        <div
          className="absolute left-0 top-0"
          style={{
            width: chartAreaW,
            height: glHeight,
            transform: rotatedTransform,
            transformOrigin: "top left",
          }}
        >
          <canvas ref={chartBaseRef} className="absolute inset-0" />
          <canvas
            ref={chartOverlayRef}
            className="pointer-events-none absolute inset-0"
          />
        </div>
      </div>

      <div
        className="absolute"
        style={{
          left: TIME_SCALE_H,
          top: 0,
          width: glHeight,
          height: PRICE_SCALE_W,
        }}
      >
        <div
          className="absolute left-0 top-0"
          style={{
            width: PRICE_SCALE_W,
            height: glHeight,
            transform: rotatedPriceTransform,
            transformOrigin: "top left",
          }}
        >
          <canvas ref={priceBaseRef} className="absolute inset-0" />
          <canvas
            ref={priceOverlayRef}
            className="pointer-events-none absolute inset-0"
          />
        </div>
      </div>

      {!mappedBars.length && !loadingBars && (
        <div
          className="absolute flex items-center justify-center text-sm"
          style={{
            left: TIME_SCALE_H,
            top: PRICE_SCALE_W,
            width: glHeight,
            height: chartAreaW,
            color: colors.emptyText,
          }}
        >
          {selectedInstrumentId
            ? "No bars for selected symbol."
            : "Search and select a symbol to render."}
        </div>
      )}
      {!!loadError && (
        <div className="absolute inset-x-0 top-2 flex items-center justify-center">
          <div className="px-3 py-1.5 rounded bg-red-500/10 text-red-300 border border-red-500/40 text-xs">
            {loadError}
          </div>
        </div>
      )}
      {!!selectorError && selectorActive && (
        <div className="absolute inset-x-0 top-12 flex items-center justify-center">
          <div
            className="px-3 py-1.5 rounded text-xs"
            style={{
              backgroundColor: colors.warningBg,
              color: colors.warningText,
              border: `1px solid ${colors.warningBorder}`,
            }}
          >
            {selectorError}
          </div>
        </div>
      )}
      {!!submitToast.message && (
        <div
          className="absolute inset-x-0 flex items-center justify-center pointer-events-none"
          style={{ top: PRICE_SCALE_W + 24 }}
        >
          <div
            className="px-3 py-1.5 rounded text-xs shadow"
            style={{
              backgroundColor:
                submitToast.tone === "success"
                  ? colors.buttonPrimaryBg
                  : colors.warningBg,
              border:
                submitToast.tone === "success"
                  ? `1px solid ${colors.buttonPrimaryBorder}`
                  : `1px solid ${colors.warningBorder}`,
              color:
                submitToast.tone === "success"
                  ? colors.priceChipText
                  : colors.warningText,
            }}
          >
            {submitToast.message}
          </div>
        </div>
      )}
    </>
  );

  // Render chart container, bottom controls, search panel, and settings modal.
  return (
    <div
      ref={containerRef}
      className={`relative w-full overflow-hidden ${className}`}
      style={{ background: activeBg }}
    >
      <div
        className={
          searchOpen || loadingBars
            ? "blur-[10px] pointer-events-none absolute top-0 left-0 right-0"
            : "absolute top-0 left-0 right-0"
        }
        style={{ bottom: BOTTOM_BAR_H, zIndex: 1 }}
      >
        {ChartArea}
      </div>

      {loadingBars && (
        <div
          className="absolute inset-x-0 top-0 flex items-center justify-center pointer-events-none"
          style={{ bottom: BOTTOM_BAR_H, zIndex: 2 }}
        >
          <div
            className="flex items-center gap-3 px-3 py-2 rounded"
            style={{
              backgroundColor: colors.chipBg,
              border: `1px solid ${colors.chipBorder}`,
              color: colors.loadingText,
            }}
          >
            <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-red-500 mx-auto" />
            <span className="text-sm">Loading {timeframe} bars...</span>
          </div>
        </div>
      )}

      {loadingMoreBars && (
        <div
          className="absolute left-6 bottom-20 px-3 py-1.5 rounded bg-blue-500/10 text-blue-300 border border-blue-500/40 text-xs"
          style={{ zIndex: 3 }}
        >
          Loading older data...
        </div>
      )}

      <div
        className="absolute left-0 bottom-0 w-full flex items-center gap-2 px-2.5"
        style={{
          height: BOTTOM_BAR_H,
          zIndex: 1000,
          backgroundColor: colors.chipBg,
          borderTop: `1px solid ${colors.chipBorder}`,
        }}
      >
        <button
          type="button"
          onClick={(e) => {
            e.stopPropagation();
            e.preventDefault();
            console.log("Settings button clicked");
            setSettingsOpen(true);
          }}
          title="Chart Settings"
          style={{
            zIndex: 1001,
            backgroundColor: colors.controlBg,
            borderColor: colors.controlBorder,
            color: colors.controlText,
          }}
          className="border rounded px-2 py-2 text-base leading-none cursor-pointer relative hover:opacity-80 transition-opacity"
        >
          ⚙️
        </button>
        <input
          readOnly
          placeholder={
            selectedInstrumentDetail
              ? `${selectedInstrumentDetail.segment} : ${selectedInstrumentDetail.trading_symbol}`
              : "Search instruments..."
          }
          onFocus={() => setSearchOpen(true)}
          style={{
            zIndex: 1001,
            backgroundColor: colors.controlBg,
            borderColor: colors.controlBorder,
            color: colors.controlText,
          }}
          className="flex-1 text-xs px-3 py-2 rounded border outline-none relative focus:border-blue-500 transition-colors"
        />
        <div className="flex items-center gap-1" style={{ zIndex: 1001 }}>
          {selectedInstrumentDetail && (
            <button
              type="button"
              disabled
              className="px-2 py-2 text-xs rounded border disabled:opacity-70 disabled:cursor-not-allowed"
              style={{
                backgroundColor: colors.chipBg,
                borderColor: colors.chipBorder,
                color: colors.controlText,
              }}
              title="Minimum price movement (tick size)"
              aria-disabled="true"
            >
              Tick: {tickSize.toFixed(tickSize < 1 ? 2 : 0)}
            </button>
          )}
          <button
            type="button"
            onClick={(e) => {
              e.stopPropagation();
              e.preventDefault();
              setSelectorActive((active) => !active);
              if (onClearOrder) onClearOrder();
            }}
            className="ml-4 px-2.5 py-2 text-xs rounded border transition-colors cursor-pointer relative hover:opacity-80"
            style={{
              backgroundColor:
                selectorActive ? colors.activeBg : colors.controlBg,
              borderColor:
                selectorActive ? colors.activeBorder : colors.controlBorder,
              color: selectorActive ? colors.activeText : colors.controlText,
            }}
          >
            Select Trade
          </button>
          <input
            type="text"
            disabled={!selectorActive}
            value={orderQty}
            onChange={(e) => setOrderQty(e.target.value)}
            placeholder="Quantity"
            className="px-2 py-2 text-xs rounded border w-20 text-center disabled:opacity-50 disabled:cursor-not-allowed"
            style={{
              backgroundColor: colors.controlBg,
              borderColor: colors.controlBorder,
              color: colors.controlText,
            }}
            title="Order quantity (enabled when trade selector is active)"
          />
          <button
            type="button"
            disabled={!selectorReady}
            onClick={(e) => {
              e.stopPropagation();
              e.preventDefault();
              handleEnterTrade();
            }}
            className="px-2.5 py-2 text-xs rounded border transition-colors cursor-pointer relative hover:opacity-80 disabled:opacity-50 disabled:cursor-not-allowed"
            style={{
              backgroundColor: selectorReady
                ? colors.buttonPrimaryBg
                : colors.controlBg,
              borderColor: selectorReady
                ? colors.buttonPrimaryBorder
                : colors.controlBorder,
              color: selectorReady ? colors.priceChipText : colors.controlText,
            }}
            title="Enter trade when all levels are set"
          >
            {submittingOrder ? "Submitting..." : "Enter Trade"}
          </button>
          <button
            type="button"
            onClick={(e) => {
              e.stopPropagation();
              e.preventDefault();
              setSelectorActive(false);
              setSelectorLevels({ entry: null, target: null, stop: null });
              setSelectorStep("entry");
              setSelectorError("");
              if (onClearOrder) onClearOrder();
            }}
            className="mr-4 px-2.5 py-2 text-xs rounded border transition-colors cursor-pointer relative hover:opacity-80"
            style={{
              backgroundColor: colors.controlBg,
              borderColor: colors.controlBorder,
              color: colors.controlText,
            }}
            title="Clear selector and re-enable crosshair"
          >
            Clear
          </button>
        </div>
        {["1m", "1D"].map((t) => (
          <button
            key={t}
            style={{
              zIndex: 1001,
              backgroundColor:
                timeframe === t ? colors.activeBg : colors.controlBg,
              borderColor:
                timeframe === t ? colors.activeBorder : colors.controlBorder,
              color: timeframe === t ? colors.activeText : colors.controlText,
            }}
            className="px-2.5 py-2 text-xs rounded border transition-colors cursor-pointer relative hover:opacity-80"
            type="button"
            onClick={(e) => {
              e.stopPropagation();
              e.preventDefault();
              console.log(`Timeframe button clicked: ${t}`);
              console.log(`Current timeframe: ${timeframe}`);
              console.log(`Selected instrument: ${selectedInstrumentId}`);

              if (!selectedInstrumentId) {
                console.warn(
                  "No instrument selected - please select an instrument first"
                );
                return;
              }
              if (timeframe === t) {
                console.log(`Already on ${t} timeframe, skipping reload`);
                return;
              }
              setTimeframe(t);
              loadBarsForInstrument(selectedInstrumentId, t);
            }}
          >
            {t}
          </button>
        ))}
      </div>
      {searchOpen && (
        <div
          className="absolute inset-0 flex items-start justify-center z-[20] pt-20"
          onClick={() => setSearchOpen(false)}
        >
          <div
            style={{
              backgroundColor: colors.searchPanelBg,
              borderColor: colors.panelBorder,
            }}
            className="pointer-events-auto rounded-md p-3 shadow-lg w-3/4 border"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center mb-3 gap-3">
              <input
                autoFocus
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                placeholder="Search instruments..."
                style={{
                  backgroundColor: colors.searchInputBg,
                  borderColor: colors.controlBorder,
                  color: colors.controlText,
                }}
                className="flex-1 text-sm px-2 py-1 rounded border outline-none focus:border-blue-500 transition-colors"
              />
              <select
                value={searchSegment}
                onChange={(e) => setSearchSegment(e.target.value)}
                style={{
                  backgroundColor: colors.searchInputBg,
                  borderColor: colors.controlBorder,
                  color: colors.controlText,
                }}
                className="text-xs px-2 py-1 rounded border outline-none transition-colors"
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
                style={{ color: colors.emptyText }}
                className="py-8 text-center text-sm"
              >
                Searching...
              </div>
            )}
            {!searchLoading && searchResults.length === 0 && (
              <div
                style={{ color: colors.emptyText }}
                className="py-8 text-center text-sm"
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
                      style={{
                        backgroundColor: "transparent",
                      }}
                      className="w-full flex items-center justify-between px-3 py-2 hover:opacity-70 transition-opacity"
                      onClick={async () => {
                        setSearchOpen(false);
                        setSearchQuery("");
                        await loadBarsForInstrument(instrument.instrument_id);
                      }}
                    >
                      <div className="flex flex-col text-left">
                        <span
                          style={{ color: colors.activeText }}
                          className="text-sm font-medium tracking-tight"
                        >
                          {instrument.description}
                        </span>
                        <span
                          style={{ color: colors.emptyText }}
                          className="text-[11px]"
                        >
                          {`${instrument.exchange} — ${instrument.trading_symbol}`}
                        </span>
                      </div>
                      <div className="flex items-center gap-2 text-[11px]">
                        <span
                          className="px-2 py-0.5 rounded"
                          style={{
                            backgroundColor: colors.badgeBg,
                            color: colors.controlText,
                          }}
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
          className="absolute inset-0 flex items-center justify-center bg-black/45"
          style={{ zIndex: 2000 }}
          onClick={(e) => {
            console.log("Settings overlay clicked");
            if (e.target === e.currentTarget) {
              console.log("Closing settings");
              setSettingsOpen(false);
            }
          }}
        >
          <div
            style={{
              backgroundColor: colors.modalBg,
              borderColor: colors.modalBorder,
            }}
            className="border rounded-lg shadow-xl w-[380px] p-4"
            onClick={(e) => e.stopPropagation()}
          >
            <div
              style={{ color: colors.activeText }}
              className="text-base font-semibold mb-3"
            >
              Chart Settings
            </div>
            <div
              style={{ color: colors.controlText }}
              className="space-y-3 text-sm"
            >
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
              <Setting label="Theme">
                <div className="flex gap-2">
                  <button
                    type="button"
                    onClick={() => setMode("dark")}
                    className={`px-3 py-1 text-xs rounded border transition-colors ${
                      themeKey === "dark"
                        ? "bg-slate-700 text-slate-100 border-slate-600"
                        : "bg-slate-100 text-slate-700 border-slate-300 hover:bg-slate-200"
                    }`}
                  >
                    Dark
                  </button>
                  <button
                    type="button"
                    onClick={() => setMode("light")}
                    className={`px-3 py-1 text-xs rounded border transition-colors ${
                      themeKey === "light"
                        ? "bg-slate-200 text-slate-900 border-slate-400"
                        : "bg-slate-100 text-slate-700 border-slate-300 hover:bg-slate-200"
                    }`}
                  >
                    Light
                  </button>
                </div>
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
                  onClick={(e) => {
                    e.stopPropagation();
                    console.log("Reset button clicked");
                    const normalizedMode = mode === "dark" ? "dark" : "light";
                    setUpCol(THEME_COLORS[normalizedMode].upColor);
                    setDownCol(THEME_COLORS[normalizedMode].downColor);
                    setVolEnabled(!!showVolume);
                    setWickScale(1.0);
                    setBarWidth(12);
                    setMode(normalizedMode);
                  }}
                  className="px-3 py-1.5 text-sm rounded cursor-pointer border transition-colors"
                  style={{
                    backgroundColor: colors.buttonSecondaryBg,
                    color: colors.activeText,
                    borderColor: colors.buttonSecondaryBorder,
                  }}
                >
                  Reset
                </button>
                <button
                  type="button"
                  onClick={(e) => {
                    e.stopPropagation();
                    console.log("Close button clicked");
                    setSettingsOpen(false);
                  }}
                  className="px-3 py-1.5 text-sm rounded cursor-pointer border transition-colors"
                  style={{
                    backgroundColor: colors.buttonPrimaryBg,
                    borderColor: colors.buttonPrimaryBorder,
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

// Renders a labeled row inside the settings modal.
function Setting({ label, children }) {
  return (
    <div className="flex items-center justify-between gap-3">
      <label className="w-40">{label}</label>
      <div className="flex items-center gap-2 flex-1">{children}</div>
    </div>
  );
}
