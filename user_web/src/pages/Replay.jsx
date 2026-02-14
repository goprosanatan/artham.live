import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import Navigate from "@components/Navigate.jsx";
import { useReplayApi } from "@components/Chart/__ReplayAPI.js";
import ChartHorizontal from "@components/Chart/Horizontal.jsx";
import { useChartApi } from "@components/Chart/__API.js";

const Replay = () => {
  const { startSession, listSessions, controlSession, deleteSession } =
    useReplayApi();
  const { filterInstrument, getInstrumentDetail } = useChartApi();
  const [searchParams, setSearchParams] = useSearchParams();

  const [sessions, setSessions] = useState([]);
  const [selectedSessionId, setSelectedSessionId] = useState("");
  const [loading, setLoading] = useState(false);
  const [busySessionId, setBusySessionId] = useState("");
  const [error, setError] = useState("");
  const [deleteConfirmSessionId, setDeleteConfirmSessionId] = useState("");
  const [instrumentSearch, setInstrumentSearch] = useState("");
  const [selectedInstrument, setSelectedInstrument] = useState(null);
  const [instruments, setInstruments] = useState([]);
  const [instrumentsLoading, setInstrumentsLoading] = useState(true);
  const [instrumentsError, setInstrumentsError] = useState("");
  const [restartCountBySession, setRestartCountBySession] = useState({});
  const [form, setForm] = useState({
    instrument_id: "",
    speed: "4",
    replay_date: "",
    start_time: "09:15",
    end_time: "15:30",
  });
  const todayDate = new Date().toISOString().slice(0, 10);

  const toTimestampMs = (dateText, timeText) => {
    if (!dateText || !timeText) return null;
    const composed = new Date(`${dateText}T${timeText}:00`);
    if (Number.isNaN(composed.getTime())) return null;
    return composed.getTime();
  };

  const refreshSessions = async () => {
    setLoading(true);
    setError("");
    try {
      const data = await listSessions();
      const sortedData = [...data].sort((a, b) => {
        const aTs = Date.parse(a?.created_at || a?.updated_at || 0);
        const bTs = Date.parse(b?.created_at || b?.updated_at || 0);
        const aSafe = Number.isNaN(aTs) ? 0 : aTs;
        const bSafe = Number.isNaN(bTs) ? 0 : bTs;
        return bSafe - aSafe;
      });
      setSessions(sortedData);
      if (
        selectedSessionId &&
        !sortedData.find((s) => s.session_id === selectedSessionId)
      ) {
        setSelectedSessionId("");
      }
    } catch (e) {
      setError(e?.message || "Failed to load replay sessions");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    refreshSessions();

    // Auto-refresh session list every 1 second to catch status changes (completed, stopped, etc.)
    const interval = setInterval(() => {
      refreshSessions();
    }, 100000);

    return () => clearInterval(interval);
  }, []);

  const loadInstruments = async () => {
    setInstrumentsLoading(true);
    setInstrumentsError("");
    try {
      const eqNse = await filterInstrument("NSE", "NSE");
      const eqBse = await filterInstrument("BSE", "BSE");
      const relianceEq = [...eqNse, ...eqBse].filter(
        (i) => i.trading_symbol === "RELIANCE",
      );

      const fut = await filterInstrument("NFO", "NFO-FUT");
      const relianceFut = fut.filter(
        (i) => i.underlying_trading_symbol === "RELIANCE",
      );

      const opt = await filterInstrument("NFO", "NFO-OPT");
      const relianceOpt = opt.filter(
        (i) => i.underlying_trading_symbol === "RELIANCE",
      );

      const combined = [
        ...relianceEq.map((instrument) => ({
          ...instrument,
          list_group: "EQ",
        })),
        ...relianceFut.map((instrument) => ({
          ...instrument,
          list_group: "FUT",
        })),
        ...relianceOpt.map((instrument) => ({
          ...instrument,
          list_group: "OPT",
        })),
      ];

      setInstruments(combined);
    } catch (err) {
      console.error("Failed to load instruments:", err);
      setInstrumentsError("Failed to load instruments");
    } finally {
      setInstrumentsLoading(false);
    }
  };

  useEffect(() => {
    loadInstruments();
  }, []);

  // Pre-fill form from URL parameters (from chart bar selection)
  useEffect(() => {
    const startParam = searchParams.get('start');
    const endParam = searchParams.get('end');
    const instrumentParam = searchParams.get('instrument');

    if (startParam && endParam) {
      // Parse timestamps to date and time
      const startTs = Number(startParam);
      const endTs = Number(endParam);

      if (Number.isFinite(startTs) && Number.isFinite(endTs)) {
        const startDate = new Date(startTs);
        const endDate = new Date(endTs);

        // Format date as YYYY-MM-DD
        const dateStr = startDate.toISOString().slice(0, 10);

        // Format times as HH:MM
        const startTimeStr = startDate.toTimeString().slice(0, 5);
        const endTimeStr = endDate.toTimeString().slice(0, 5);

        // Update form
        setForm((prev) => ({
          ...prev,
          replay_date: dateStr,
          start_time: startTimeStr,
          end_time: endTimeStr,
          instrument_id: instrumentParam || prev.instrument_id,
        }));

        // Load and select instrument if provided
        if (instrumentParam) {
          const instrumentId = Number(instrumentParam);
          if (Number.isFinite(instrumentId)) {
            getInstrumentDetail(instrumentId)
              .then((instrument) => {
                if (instrument) {
                  setSelectedInstrument(instrument);
                  setInstrumentSearch(
                    instrument.trading_symbol || instrument.description || ""
                  );
                }
              })
              .catch((err) => {
                console.error("Failed to load instrument:", err);
              });
          }
        }

        // Clear URL params after reading to clean up URL
        setSearchParams({});
      }
    }
  }, [searchParams, setSearchParams, getInstrumentDetail]);

  const handleInstrumentSelect = (instrument) => {
    setSelectedInstrument(instrument);
    setForm((prev) => ({
      ...prev,
      instrument_id: String(instrument.instrument_id || ""),
    }));
    setInstrumentSearch(
      instrument.trading_symbol || instrument.description || "",
    );
  };

  const onStartSession = async () => {
    setError("");
    try {
      const startTs = toTimestampMs(form.replay_date, form.start_time);
      const endTs = toTimestampMs(form.replay_date, form.end_time);

      if (!form.replay_date) {
        setError("Please select a replay date");
        return;
      }
      if (!form.instrument_id) {
        setError("Please select an instrument");
        return;
      }
      if (!form.start_time || !form.end_time) {
        setError("Please select both start and end time");
        return;
      }
      if (startTs == null || endTs == null) {
        setError("Invalid date/time selection");
        return;
      }
      if (endTs <= startTs) {
        setError("End time must be later than start time");
        return;
      }

      const payload = {
        instrument_id: form.instrument_id ? Number(form.instrument_id) : null,
        speed: Number(form.speed || 1),
        timestamp_start: startTs,
        timestamp_end: endTs,
      };
      const created = await startSession(payload);
      if (created?.session_id) {
        setSelectedSessionId(created.session_id);
      }
      await refreshSessions();
    } catch (e) {
      setError(e?.message || "Failed to start replay session");
    }
  };

  const onControl = async (session_id, action) => {
    setBusySessionId(session_id);
    setError("");
    try {
      // Increment restart counter before the restart action completes
      if (action === "restart") {
        setRestartCountBySession((prev) => ({
          ...prev,
          [session_id]: (prev[session_id] || 0) + 1,
        }));
      }
      
      await controlSession(session_id, action);
      await refreshSessions();
    } catch (e) {
      setError(e?.message || `Failed to ${action} replay session`);
      // Reset restart counter if the action failed
      if (action === "restart") {
        setRestartCountBySession((prev) => ({
          ...prev,
          [session_id]: (prev[session_id] || 0) - 1,
        }));
      }
    } finally {
      setBusySessionId("");
    }
  };

  const onDeleteSession = async (session_id) => {
    setBusySessionId(session_id);
    setError("");
    try {
      await deleteSession(session_id);
      if (selectedSessionId === session_id) {
        setSelectedSessionId("");
      }
      await refreshSessions();
      setDeleteConfirmSessionId("");
    } catch (e) {
      setError(e?.message || "Failed to delete replay session");
    } finally {
      setBusySessionId("");
    }
  };

  const selectedSession =
    sessions.find((session) => session.session_id === selectedSessionId) ||
    null;
  const selectedInstrumentId = Number(selectedSession?.instrument_id);
  const replayBarsTimestampEnd = Number(selectedSession?.timestamp_start);
  const replaySlotsTimestampEnd = null;
  const previewStartTs = toTimestampMs(form.replay_date, form.start_time);
  const previewEndTs = toTimestampMs(form.replay_date, form.end_time);
  const isInstrumentMissing = !form.instrument_id;
  const normalizedSearch = instrumentSearch.trim().toLowerCase();
  const filteredInstruments = normalizedSearch
    ? instruments.filter((instrument) => {
        const haystack = [
          instrument.trading_symbol,
          instrument.description,
          instrument.underlying_trading_symbol,
          instrument.exchange,
          instrument.segment,
          instrument.instrument_type,
          instrument.instrument_id,
        ]
          .filter(Boolean)
          .join(" ")
          .toLowerCase();
        return haystack.includes(normalizedSearch);
      })
    : instruments;
  const instrumentById = new Map(
    instruments.map((instrument) => [
      String(instrument.instrument_id),
      instrument,
    ]),
  );
  const canStartReplay =
    !isInstrumentMissing &&
    !!form.replay_date &&
    !!form.start_time &&
    !!form.end_time &&
    previewStartTs != null &&
    previewEndTs != null &&
    previewEndTs > previewStartTs;

  const startReplayHint = !form.replay_date
    ? "Select a replay date"
    : !form.instrument_id
      ? "Select an instrument"
      : !form.start_time || !form.end_time
        ? "Select both start and end time"
        : previewStartTs == null || previewEndTs == null
          ? "Use a valid date/time"
          : previewEndTs <= previewStartTs
            ? "End time must be after start time"
            : "Ready to start replay";

  return (
    <div className="flex flex-col h-screen p-1 gap-1 dark:bg-gray-500">
      <Navigate className="basis-12 border-solid border-black rounded-lg" />

      <div className="grow flex gap-1 overflow-hidden">
        <div className="w-[360px] shrink-0 rounded-lg border border-gray-300 bg-white/70 dark:bg-gray-700 dark:border-gray-600 p-4 overflow-auto">
          <div className="text-xl font-semibold text-gray-900 dark:text-gray-100">
            Replay
          </div>

          <div className="mt-4 grid grid-cols-1 gap-3">
            <label className="text-sm text-gray-800 dark:text-gray-100">
              Instrument
              <input
                className={`w-full mt-1 px-2 py-1 rounded border ${
                  isInstrumentMissing
                    ? "border-red-400 focus:border-red-500 focus:ring-1 focus:ring-red-200 dark:border-red-500"
                    : "border-gray-300 dark:border-gray-600"
                }`}
                placeholder="Search trading symbol"
                value={instrumentSearch}
                onChange={(e) => setInstrumentSearch(e.target.value)}
              />
            </label>

            <div
              className={`rounded border ${
                isInstrumentMissing
                  ? "border-red-300 dark:border-red-500"
                  : "border-gray-200 dark:border-gray-600"
              }`}
            >
              <div className="max-h-64 overflow-auto">
                {instrumentsLoading ? (
                  <div className="p-3 text-xs text-gray-600 dark:text-gray-300">
                    Loading...
                  </div>
                ) : instrumentsError ? (
                  <div className="p-3 text-xs text-red-600 dark:text-red-300">
                    {instrumentsError}
                  </div>
                ) : filteredInstruments.length === 0 ? (
                  <div className="p-3 text-xs text-gray-600 dark:text-gray-300">
                    {normalizedSearch ? "No matches" : "No instruments"}
                  </div>
                ) : (
                  <ul className="divide-y divide-gray-200 dark:divide-gray-600">
                    {filteredInstruments.map((instrument) => {
                      const isSelected =
                        selectedInstrument?.instrument_id ===
                        instrument.instrument_id;
                      return (
                        <li key={instrument.instrument_id}>
                          <button
                            type="button"
                            onClick={() => handleInstrumentSelect(instrument)}
                            className={`w-full text-left px-3 py-2 text-xs hover:bg-gray-100 dark:hover:bg-gray-700 ${
                              isSelected
                                ? "bg-blue-50 dark:bg-blue-900/30"
                                : "bg-transparent"
                            }`}
                          >
                            <div className="font-semibold text-gray-800 dark:text-gray-50">
                              {instrument.trading_symbol ||
                                instrument.description}
                            </div>
                            <div className="text-gray-600 dark:text-gray-300">
                              {instrument.exchange} · {instrument.list_group} ·
                              ID {instrument.instrument_id}
                            </div>
                          </button>
                        </li>
                      );
                    })}
                  </ul>
                )}
              </div>
            </div>

            {isInstrumentMissing && (
              <div className="text-xs text-red-600 dark:text-red-300">
                Please select an instrument
              </div>
            )}

            {selectedInstrument && (
              <div className="text-xs text-gray-600 dark:text-gray-300">
                Selected:{" "}
                {selectedInstrument.trading_symbol ||
                  selectedInstrument.description}{" "}
                · ID {selectedInstrument.instrument_id}
              </div>
            )}

            <label className="text-sm text-gray-800 dark:text-gray-100">
              Speed
              <div className="mt-1 flex items-center gap-2">
                <input
                  type="range"
                  min="0.25"
                  max="10"
                  step="0.25"
                  value={form.speed}
                  onChange={(e) =>
                    setForm((prev) => ({ ...prev, speed: e.target.value }))
                  }
                  className="w-full"
                />
                <span className="text-xs text-gray-700 dark:text-gray-200 min-w-[48px] text-right">
                  {Number(form.speed).toFixed(2)}x
                </span>
              </div>
            </label>

            <label className="text-sm text-gray-800 dark:text-gray-100">
              Replay Date
              <input
                type="date"
                className="w-full mt-1 px-2 py-1 rounded border"
                value={form.replay_date}
                max={todayDate}
                onChange={(e) =>
                  setForm((prev) => ({ ...prev, replay_date: e.target.value }))
                }
              />
            </label>

            <label className="text-sm text-gray-800 dark:text-gray-100">
              Start Time
              <input
                type="time"
                className="w-full mt-1 px-2 py-1 rounded border"
                value={form.start_time}
                onChange={(e) =>
                  setForm((prev) => ({ ...prev, start_time: e.target.value }))
                }
              />
            </label>

            <label className="text-sm text-gray-800 dark:text-gray-100">
              End Time
              <input
                type="time"
                className="w-full mt-1 px-2 py-1 rounded border"
                value={form.end_time}
                onChange={(e) =>
                  setForm((prev) => ({ ...prev, end_time: e.target.value }))
                }
              />
            </label>

            <div className="flex gap-2">
              <button
                type="button"
                onClick={onStartSession}
                disabled={!canStartReplay}
                className="px-3 py-1.5 rounded bg-blue-600 text-white text-sm disabled:opacity-60 disabled:cursor-not-allowed"
              >
                Start Replay
              </button>
              <button
                type="button"
                onClick={refreshSessions}
                className="px-3 py-1.5 rounded bg-gray-600 text-white text-sm"
              >
                Refresh
              </button>
            </div>
          </div>

          <div
            className={`mt-2 text-xs ${
              canStartReplay
                ? "text-green-700 dark:text-green-300"
                : "text-gray-700 dark:text-gray-300"
            }`}
          >
            {startReplayHint}
          </div>

          {error && (
            <div className="mt-3 text-sm text-red-600 dark:text-red-300">
              {error}
            </div>
          )}

          <div className="mt-4">
            <div className="text-sm font-semibold text-gray-900 dark:text-gray-100">
              Sessions
            </div>
            {loading ? (
              <div className="mt-2 text-sm text-gray-700 dark:text-gray-300">
                Loading...
              </div>
            ) : sessions.length === 0 ? (
              <div className="mt-2 text-sm text-gray-700 dark:text-gray-300">
                No replay sessions
              </div>
            ) : (
              <div className="mt-2 space-y-2">
                {sessions.map((session) => (
                  <div
                    key={session.session_id}
                    className="rounded border border-gray-300 dark:border-gray-600 px-3 py-2"
                  >
                    {(() => {
                      const matchedInstrument = instrumentById.get(
                        String(session.instrument_id),
                      );
                      const sessionSymbol =
                        matchedInstrument?.trading_symbol ||
                        matchedInstrument?.description ||
                        (session.instrument_id
                          ? `ID ${session.instrument_id}`
                          : "Unknown instrument");

                      const getStatusColor = (status) => {
                        // Status values from backend:
                        // - "running": Session actively publishing ticks
                        // - "paused": Session paused by user (pause action)
                        // - "completed": Session finished successfully or stopped by user
                        // - "failed": Session error (missing params, no ticks, etc.)
                        switch (status) {
                          case "running":
                            return "bg-blue-600 dark:bg-blue-700 text-white";
                          case "paused":
                            return "bg-amber-500 dark:bg-amber-600 text-white";
                          case "completed":
                            return "bg-green-600 dark:bg-green-700 text-white";
                          case "failed":
                            return "bg-red-600 dark:bg-red-700 text-white";
                          default:
                            return "bg-gray-400 dark:bg-gray-600 text-white";
                        }
                      };

                      return (
                        <div className="flex items-center justify-between text-sm text-gray-900 dark:text-gray-100">
                          <span>
                            {sessionSymbol} · speed {session.speed}
                          </span>
                          <span
                            className={`px-3 py-1 rounded-full text-xs font-medium whitespace-nowrap ${getStatusColor(session.status)}`}
                          >
                            {session.status}
                          </span>
                        </div>
                      );
                    })()}
                    <div className="mt-2 flex gap-2 flex-wrap">
                      <button
                        type="button"
                        onClick={() => setSelectedSessionId(session.session_id)}
                        className={`px-2 py-1 text-xs rounded text-white ${
                          selectedSessionId === session.session_id
                            ? "bg-blue-700"
                            : "bg-blue-600"
                        }`}
                      >
                        Open
                      </button>
                      <button
                        type="button"
                        onClick={() => onControl(session.session_id, "pause")}
                        disabled={
                          busySessionId === session.session_id ||
                          session.status !== "running" ||
                          session.status === "completed" ||
                          session.status === "failed"
                        }
                        className="px-2 py-1 text-xs rounded bg-amber-500 text-white disabled:opacity-60 disabled:cursor-not-allowed"
                        title={
                          session.status !== "running"
                            ? "Can only pause a running session"
                            : ""
                        }
                      >
                        Pause
                      </button>
                      <button
                        type="button"
                        onClick={() => onControl(session.session_id, "resume")}
                        disabled={
                          busySessionId === session.session_id ||
                          session.status !== "paused" ||
                          session.status === "completed" ||
                          session.status === "failed"
                        }
                        className="px-2 py-1 text-xs rounded bg-green-600 text-white disabled:opacity-60 disabled:cursor-not-allowed"
                        title={
                          session.status !== "paused"
                            ? "Can only resume a paused session"
                            : ""
                        }
                      >
                        Resume
                      </button>
                      <button
                        type="button"
                        onClick={() => onControl(session.session_id, "restart")}
                        disabled={busySessionId === session.session_id}
                        className="px-2 py-1 text-xs rounded bg-blue-500 text-white disabled:opacity-60 disabled:cursor-not-allowed"
                        title="Restart the replay from the beginning"
                      >
                        Restart
                      </button>
                      {deleteConfirmSessionId === session.session_id ? (
                        <>
                          <button
                            type="button"
                            onClick={() => onDeleteSession(session.session_id)}
                            disabled={busySessionId === session.session_id}
                            className="px-2 py-1 text-xs rounded bg-red-700 text-white disabled:opacity-60 disabled:cursor-not-allowed"
                          >
                            Confirm Delete
                          </button>
                          <button
                            type="button"
                            onClick={() => setDeleteConfirmSessionId("")}
                            className="px-2 py-1 text-xs rounded bg-gray-500 text-white"
                          >
                            Cancel
                          </button>
                        </>
                      ) : (
                        <button
                          type="button"
                          onClick={() =>
                            setDeleteConfirmSessionId(session.session_id)
                          }
                          disabled={busySessionId === session.session_id}
                          className="px-2 py-1 text-xs rounded bg-gray-600 text-white disabled:opacity-60 disabled:cursor-not-allowed hover:bg-gray-700"
                        >
                          Delete
                        </button>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>

        <div className="flex-1 min-w-0 rounded-lg border border-gray-300 bg-white/70 dark:bg-gray-700 dark:border-gray-600 overflow-hidden">
          {selectedSession && Number.isFinite(selectedInstrumentId) ? (
            <ChartHorizontal
              key={`${selectedSession.session_id}-${restartCountBySession[selectedSession.session_id] || 0}`}
              instrumentId={selectedInstrumentId}
              replaySessionId={selectedSession.session_id}
              replayBarsTimestampEnd={
                Number.isFinite(replayBarsTimestampEnd)
                  ? replayBarsTimestampEnd
                  : null
              }
              replaySlotsTimestampEnd={
                Number.isFinite(replaySlotsTimestampEnd)
                  ? replaySlotsTimestampEnd
                  : null
              }
              className="h-full rounded-lg"
              is_intraday={false}
              showVolume
            />
          ) : (
            <div className="h-full flex items-center justify-center p-4">
              <div className="text-center text-xl font-medium text-gray-700 dark:text-gray-300 max-w-md">
                <span className="block">Fill the replay session with a valid instrument</span>
                <span className="block">Or Select an already created session</span>
                <span className="block">to open chart streaming.</span>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default Replay;
