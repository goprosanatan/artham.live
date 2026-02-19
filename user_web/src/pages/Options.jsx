import { useEffect, useMemo, useState } from "react";
import Navigate from "@components/Navigate.jsx";
import { useAuth } from "@contexts/authProvider.jsx";
import { requestApi } from "@hooks/requestApi.js";
import { useChartApi } from "@components/Chart/__API.js";
import { connectLive } from "@components/Chart/__Websocket.js";

const toNum = (value) => {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const formatNum = (value, digits = 2) => {
  const parsed = toNum(value);
  return parsed == null ? "--" : parsed.toFixed(digits);
};

const formatIv = (value) => {
  const parsed = toNum(value);
  return parsed == null ? "--" : `${parsed.toFixed(2)}%`;
};

const getCeMoneyness = (strike, spot, atmStrike) => {
  if (spot == null || atmStrike == null) return "--";
  if (strike === atmStrike) return "ATM";
  return strike < spot ? "ITM" : "OTM";
};

const getPeMoneyness = (strike, spot, atmStrike) => {
  if (spot == null || atmStrike == null) return "--";
  if (strike === atmStrike) return "ATM";
  return strike > spot ? "ITM" : "OTM";
};

const Options = () => {
  const { token } = useAuth();
  const { request } = requestApi();
  const { getDerivatives } = useChartApi();

  const [profile, setProfile] = useState({
    email_id: "loading...",
    full_name: "loading...",
  });

  const [exchange, setExchange] = useState("NFO");
  const [symbolInput, setSymbolInput] = useState("RELIANCE");
  const [symbol, setSymbol] = useState("RELIANCE");
  const [chain, setChain] = useState({ options: {}, futures: [] });
  const [expiries, setExpiries] = useState([]);
  const [selectedExpiry, setSelectedExpiry] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [liveByInstrument, setLiveByInstrument] = useState({});
  const [underlyingLastPrice, setUnderlyingLastPrice] = useState(null);

  useEffect(() => {
    const loadProfile = async () => {
      try {
        const response = await request("user/dashboard", "POST");
        if (response?.profile) {
          setProfile(response.profile);
        }
      } catch {
        // Protected route handles auth redirect; no extra action needed here.
      }
    };

    loadProfile();
  }, []);

  const loadDerivatives = async (nextSymbol = symbol) => {
    setLoading(true);
    setError("");
    try {
      const payload = await getDerivatives(exchange, nextSymbol);
      const options = payload?.options || {};
      const nextExpiries = Object.keys(options).sort((a, b) =>
        new Date(a).getTime() - new Date(b).getTime()
      );

      setChain({
        options,
        futures: Array.isArray(payload?.futures) ? payload.futures : [],
      });
      setExpiries(nextExpiries);
      setSelectedExpiry((prev) => {
        if (prev && nextExpiries.includes(prev)) return prev;
        return nextExpiries[0] || "";
      });
      setLiveByInstrument({});
      setUnderlyingLastPrice(null);
    } catch (err) {
      setError(err?.message || "Failed to load option chain");
      setChain({ options: {}, futures: [] });
      setExpiries([]);
      setSelectedExpiry("");
      setLiveByInstrument({});
      setUnderlyingLastPrice(null);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadDerivatives(symbol);
  }, [exchange]);

  const strikeRows = useMemo(() => {
    const byStrike = chain?.options?.[selectedExpiry] || {};
    return Object.keys(byStrike)
      .sort((a, b) => Number(a) - Number(b))
      .map((strike) => {
        const strikeNode = byStrike[strike] || {};
        const ceInst = strikeNode.CE || null;
        const peInst = strikeNode.PE || null;

        return {
          strike,
          ceInst,
          peInst,
          ceLive: ceInst ? liveByInstrument[String(ceInst.instrument_id)] : null,
          peLive: peInst ? liveByInstrument[String(peInst.instrument_id)] : null,
        };
      });
  }, [chain, selectedExpiry, liveByInstrument]);

  const underlyingInstrumentId = useMemo(() => {
    const strikeMap = chain?.options?.[selectedExpiry] || {};
    for (const strikeNode of Object.values(strikeMap)) {
      const ceId = toNum(strikeNode?.CE?.underlying_instrument_id);
      if (ceId != null) return ceId;
      const peId = toNum(strikeNode?.PE?.underlying_instrument_id);
      if (peId != null) return peId;
    }
    return null;
  }, [chain, selectedExpiry]);

  const atmStrike = useMemo(() => {
    if (underlyingLastPrice == null || !strikeRows.length) return null;
    let best = null;
    let minDiff = Number.POSITIVE_INFINITY;
    for (const row of strikeRows) {
      const strikeNum = toNum(row.strike);
      if (strikeNum == null) continue;
      const diff = Math.abs(strikeNum - underlyingLastPrice);
      if (diff < minDiff) {
        minDiff = diff;
        best = strikeNum;
      }
    }
    return best;
  }, [underlyingLastPrice, strikeRows]);

  useEffect(() => {
    if (!token || !selectedExpiry) return;

    const strikeMap = chain?.options?.[selectedExpiry] || {};
    const instrumentIds = [];

    for (const strikeNode of Object.values(strikeMap)) {
      if (strikeNode?.CE?.instrument_id != null) {
        instrumentIds.push(Number(strikeNode.CE.instrument_id));
      }
      if (strikeNode?.PE?.instrument_id != null) {
        instrumentIds.push(Number(strikeNode.PE.instrument_id));
      }
    }

    const uniqueInstrumentIds = Array.from(
      new Set(instrumentIds.filter((id) => Number.isFinite(id)))
    );

    if (!uniqueInstrumentIds.length) return;

    const instrumentIdSet = new Set(uniqueInstrumentIds.map((id) => String(id)));
    let subscribed = false;

    const { disconnect, subscribe, unsubscribe } = connectLive({
      token,
      onConnect: () => {
        console.log("[Options][WS] connected (option stream)");
      },
      onAuthenticated: () => {
        console.log("[Options][WS] authenticated (option stream)");
        subscribe?.(uniqueInstrumentIds, "feature.option_chain");
        subscribed = true;
      },
      onSubscribed: (data) => {
        console.log("[Options][WS] subscribed", data);
      },
      onUnsubscribed: (data) => {
        console.log("[Options][WS] unsubscribed", data);
      },
      onError: (err) => {
        console.error("[Options][WS] error", err);
      },
      onUnauthorized: (data) => {
        console.error("[Options][WS] unauthorized", data);
      },
      onOptionFeature: (payload) => {
        console.log("[Options][WS][option_feature] raw", payload);
        if (!payload) return;
        if (payload.type && payload.type !== "feature.option_chain") return;

        const data = payload.data || payload;
        const instrumentId = data?.instrument_id;
        console.log("[Options][WS][option_feature] parsed", {
          instrument_id: instrumentId,
          option_price: data?.option_price,
          implied_vol: data?.implied_vol,
          delta: data?.delta,
          gamma: data?.gamma,
          theta: data?.theta,
          vega: data?.vega,
          rho: data?.rho,
        });
        if (instrumentId == null) return;

        const key = String(instrumentId);
        if (!instrumentIdSet.has(key)) return;

        setLiveByInstrument((prev) => ({
          ...prev,
          [key]: {
            instrument_id: toNum(data.instrument_id),
            option_type: data.option_type,
            strike: toNum(data.strike),
            expiry: data.expiry,
            underlying_price: toNum(data.underlying_price),
            option_price: toNum(data.option_price),
            implied_vol: toNum(data.implied_vol),
            theoretical_price: toNum(data.theoretical_price),
            delta: toNum(data.delta),
            gamma: toNum(data.gamma),
            vega: toNum(data.vega),
            theta: toNum(data.theta),
            rho: toNum(data.rho),
            exchange_ts: data.exchange_ts || null,
            ingest_ts: data.ingest_ts || null,
          },
        }));
      },
      onDisconnect: () => {
        subscribed = false;
      },
    });

    return () => {
      if (subscribed) {
        unsubscribe?.(uniqueInstrumentIds, "feature.option_chain");
      }
      disconnect();
    };
  }, [token, chain, selectedExpiry]);

  useEffect(() => {
    const underlyingId = toNum(underlyingInstrumentId);
    if (!token || underlyingId == null) return;

    let subscribed = false;
    const { disconnect, subscribe, unsubscribe } = connectLive({
      token,
      onAuthenticated: () => {
        console.log("[Options][WS] authenticated (equity depth)");
        subscribe?.([underlyingId], "feature.equity_depth");
        subscribed = true;
      },
      onSubscribed: (data) => {
        console.log("[Options][WS] subscribed (equity)", data);
      },
      onError: (err) => {
        console.error("[Options][WS] error (equity)", err);
      },
      onDepth: (payload) => {
        console.log("[Options][WS][equity_depth] raw", payload);
        if (!payload) return;
        if (payload.type && payload.type !== "feature.equity_depth") return;

        const data = payload.data || payload;
        const payloadInstrumentId = toNum(data?.instrument_id);
        if (payloadInstrumentId == null || payloadInstrumentId !== underlyingId) return;

        const ltp = toNum(data?.last_price);
        console.log("[Options][WS][equity_depth] parsed", {
          instrument_id: payloadInstrumentId,
          last_price: data?.last_price,
          parsed_last_price: ltp,
        });
        if (ltp != null) setUnderlyingLastPrice(ltp);
      },
      onDisconnect: () => {
        subscribed = false;
      },
    });

    return () => {
      if (subscribed) {
        unsubscribe?.([underlyingId], "feature.equity_depth");
      }
      disconnect();
    };
  }, [token, underlyingInstrumentId]);

  return (
    <div className="flex flex-col h-screen p-1 gap-1 bg-gradient-to-br from-sky-100 via-indigo-100 to-rose-100 dark:from-slate-900 dark:via-slate-800 dark:to-slate-900">
      <Navigate className="basis-12 border-solid border-black rounded-lg" profile={profile} />

      <div className="grow min-h-0 rounded-lg border border-indigo-300 bg-white/75 dark:bg-gray-700/90 dark:border-indigo-700 flex flex-col overflow-hidden shadow-xl">
        <div className="px-4 py-3 border-b border-indigo-200 dark:border-indigo-700 flex flex-wrap items-end gap-3 bg-gradient-to-r from-cyan-100 via-indigo-100 to-pink-100 dark:from-slate-800 dark:via-slate-700 dark:to-slate-800">
          <div className="flex flex-col">
            <label className="text-xs text-gray-600 dark:text-gray-200">Exchange</label>
            <select
              value={exchange}
              onChange={(event) => setExchange(event.target.value)}
              className="px-2 py-1 rounded border border-gray-300 dark:border-gray-500 dark:bg-gray-800 dark:text-gray-100 text-sm"
            >
              <option value="NFO">NFO</option>
            </select>
          </div>

          <div className="flex flex-col">
            <label className="text-xs text-gray-600 dark:text-gray-200">Underlying Symbol</label>
            <input
              value={symbolInput}
              onChange={(event) => setSymbolInput(event.target.value.toUpperCase())}
              className="px-2 py-1 rounded border border-gray-300 dark:border-gray-500 dark:bg-gray-800 dark:text-gray-100 text-sm"
              placeholder="RELIANCE"
            />
          </div>

          <button
            type="button"
            onClick={() => {
              const next = symbolInput.trim().toUpperCase();
              if (!next) return;
              setSymbol(next);
              loadDerivatives(next);
            }}
            disabled={loading}
            className="px-3 py-1.5 rounded bg-blue-600 text-white text-sm disabled:opacity-60"
          >
            {loading ? "Loading..." : "Load Chain"}
          </button>

          <div className="flex flex-col">
            <label className="text-xs text-gray-600 dark:text-gray-200">Expiry</label>
            <select
              value={selectedExpiry}
              onChange={(event) => {
                setSelectedExpiry(event.target.value);
                setLiveByInstrument({});
                setUnderlyingLastPrice(null);
              }}
              className="px-2 py-1 rounded border border-gray-300 dark:border-gray-500 dark:bg-gray-800 dark:text-gray-100 text-sm"
              disabled={!expiries.length}
            >
              {!expiries.length ? (
                <option value="">No expiry</option>
              ) : (
                expiries.map((expiry) => (
                  <option key={expiry} value={expiry}>
                    {expiry}
                  </option>
                ))
              )}
            </select>
          </div>

          <div className="text-xs font-semibold text-indigo-700 dark:text-indigo-200 ml-auto">
            {symbol} | strikes: {strikeRows.length}
          </div>
        </div>
        <div className="px-4 py-2 border-b border-indigo-100 dark:border-indigo-800 text-xs font-semibold bg-white/60 dark:bg-slate-800/70 flex flex-wrap gap-4">
          <span className="text-indigo-700 dark:text-indigo-200">
            Spot: {formatNum(underlyingLastPrice)}
          </span>
          <span className="text-amber-700 dark:text-amber-300">
            ATM Strike: {atmStrike == null ? "--" : atmStrike}
          </span>
          <span className="text-emerald-700 dark:text-emerald-300">CE ITM/OTM shown near strike</span>
          <span className="text-rose-700 dark:text-rose-300">PE ITM/OTM shown near strike</span>
        </div>

        {error ? (
          <div className="px-4 py-3 text-sm text-red-600 dark:text-red-300">{error}</div>
        ) : !selectedExpiry ? (
          <div className="h-full flex items-center justify-center text-sm text-gray-600 dark:text-gray-300">
            {loading ? "Loading option chain..." : "No option chain found"}
          </div>
        ) : (
          <div className="flex-1 min-h-0 overflow-auto">
            <table className="w-full text-xs border-collapse">
              <thead className="sticky top-0 bg-gray-200 dark:bg-gray-800 text-gray-700 dark:text-gray-200">
                <tr>
                  <th className="px-2 py-2 border-b border-emerald-300 dark:border-emerald-700 text-left bg-emerald-100/80 dark:bg-emerald-900/60">CE Vega</th>
                  <th className="px-2 py-2 border-b border-emerald-300 dark:border-emerald-700 text-left bg-emerald-100/80 dark:bg-emerald-900/60">CE Theta</th>
                  <th className="px-2 py-2 border-b border-emerald-300 dark:border-emerald-700 text-left bg-emerald-100/80 dark:bg-emerald-900/60">CE Gamma</th>
                  <th className="px-2 py-2 border-b border-emerald-300 dark:border-emerald-700 text-left bg-emerald-100/80 dark:bg-emerald-900/60">CE Delta</th>
                  <th className="px-2 py-2 border-b border-emerald-300 dark:border-emerald-700 text-left bg-emerald-100/80 dark:bg-emerald-900/60">CE IV</th>
                  <th className="px-2 py-2 border-b border-emerald-300 dark:border-emerald-700 text-center bg-emerald-100/80 dark:bg-emerald-900/60">CE MNY</th>
                  <th className="px-2 py-2 border-b border-emerald-300 dark:border-emerald-700 text-left bg-emerald-200/90 dark:bg-emerald-800/70 font-bold">CE LTP</th>
                  <th className="px-2 py-2 border-b border-indigo-300 dark:border-indigo-700 text-center bg-indigo-200/90 dark:bg-indigo-900/80 font-bold">Strike</th>
                  <th className="px-2 py-2 border-b border-rose-300 dark:border-rose-700 text-left bg-rose-200/90 dark:bg-rose-800/70 font-bold">PE LTP</th>
                  <th className="px-2 py-2 border-b border-rose-300 dark:border-rose-700 text-center bg-rose-100/80 dark:bg-rose-900/60">PE MNY</th>
                  <th className="px-2 py-2 border-b border-rose-300 dark:border-rose-700 text-left bg-rose-100/80 dark:bg-rose-900/60">PE IV</th>
                  <th className="px-2 py-2 border-b border-rose-300 dark:border-rose-700 text-left bg-rose-100/80 dark:bg-rose-900/60">PE Delta</th>
                  <th className="px-2 py-2 border-b border-rose-300 dark:border-rose-700 text-left bg-rose-100/80 dark:bg-rose-900/60">PE Gamma</th>
                  <th className="px-2 py-2 border-b border-rose-300 dark:border-rose-700 text-left bg-rose-100/80 dark:bg-rose-900/60">PE Theta</th>
                  <th className="px-2 py-2 border-b border-rose-300 dark:border-rose-700 text-left bg-rose-100/80 dark:bg-rose-900/60">PE Vega</th>
                </tr>
              </thead>
              <tbody>
                {strikeRows.map((row) => (
                  <tr
                    key={`${selectedExpiry}-${row.strike}`}
                    className={`odd:bg-white/55 even:bg-indigo-50/60 hover:bg-yellow-50/70 dark:odd:bg-gray-700/35 dark:even:bg-gray-700/65 dark:hover:bg-gray-600/80 transition-colors ${
                      toNum(row.strike) === atmStrike ? "ring-2 ring-amber-400 dark:ring-amber-500" : ""
                    }`}
                  >
                    <td className="px-2 py-1.5 tabular-nums text-emerald-800 dark:text-emerald-300">{formatNum(row.ceLive?.vega, 4)}</td>
                    <td className="px-2 py-1.5 tabular-nums text-emerald-800 dark:text-emerald-300">{formatNum(row.ceLive?.theta, 4)}</td>
                    <td className="px-2 py-1.5 tabular-nums text-emerald-800 dark:text-emerald-300">{formatNum(row.ceLive?.gamma, 6)}</td>
                    <td className="px-2 py-1.5 tabular-nums text-emerald-800 dark:text-emerald-300">{formatNum(row.ceLive?.delta, 4)}</td>
                    <td className="px-2 py-1.5 tabular-nums text-emerald-900 dark:text-emerald-200">{formatIv(row.ceLive?.implied_vol)}</td>
                    <td className="px-2 py-1.5 text-center">
                      <div
                        className={`inline-flex min-w-14 items-center justify-center px-1.5 py-0.5 rounded border text-[10px] font-bold ${
                          getCeMoneyness(toNum(row.strike), underlyingLastPrice, atmStrike) === "ITM"
                            ? "bg-[linear-gradient(135deg,#d1fae5_0%,#d1fae5_48%,#10b981_49%,#10b981_52%,#ecfdf5_53%,#ecfdf5_100%)] border-emerald-300 text-emerald-900 dark:bg-[linear-gradient(135deg,#064e3b_0%,#064e3b_48%,#10b981_49%,#10b981_52%,#022c22_53%,#022c22_100%)] dark:border-emerald-700 dark:text-emerald-100"
                            : getCeMoneyness(toNum(row.strike), underlyingLastPrice, atmStrike) === "OTM"
                            ? "bg-[linear-gradient(135deg,#ecfdf5_0%,#ecfdf5_48%,#34d399_49%,#34d399_52%,#d1fae5_53%,#d1fae5_100%)] border-emerald-200 text-emerald-700 dark:bg-[linear-gradient(135deg,#022c22_0%,#022c22_48%,#34d399_49%,#34d399_52%,#064e3b_53%,#064e3b_100%)] dark:border-emerald-800 dark:text-emerald-300"
                            : "bg-amber-100 border-amber-300 text-amber-900 dark:bg-amber-900 dark:border-amber-700 dark:text-amber-100"
                        }`}
                      >
                        {getCeMoneyness(toNum(row.strike), underlyingLastPrice, atmStrike)}
                      </div>
                    </td>
                    <td className="px-2 py-1.5 tabular-nums font-bold text-emerald-900 dark:text-emerald-100">{formatNum(row.ceLive?.option_price)}</td>
                    <td
                      className={`px-2 py-1.5 text-center font-bold tabular-nums text-indigo-800 dark:text-indigo-200 ${
                        toNum(row.strike) === atmStrike
                          ? "bg-[linear-gradient(135deg,#d1fae5_0%,#d1fae5_47%,#fcd34d_48%,#fcd34d_52%,#ffe4e6_53%,#ffe4e6_100%)] dark:bg-[linear-gradient(135deg,#064e3b_0%,#064e3b_47%,#b45309_48%,#b45309_52%,#881337_53%,#881337_100%)]"
                          : "bg-indigo-100/70 dark:bg-indigo-900/60"
                      }`}
                    >
                      <span>{row.strike}</span>
                      {toNum(row.strike) === atmStrike ? (
                        <span className="ml-2 px-1.5 py-0.5 rounded bg-amber-200 text-amber-900 dark:bg-amber-700 dark:text-amber-100 text-[10px] align-middle">
                          ATM
                        </span>
                      ) : null}
                    </td>
                    <td className="px-2 py-1.5 tabular-nums font-bold text-rose-900 dark:text-rose-100">{formatNum(row.peLive?.option_price)}</td>
                    <td className="px-2 py-1.5 text-center">
                      <div
                        className={`inline-flex min-w-14 items-center justify-center px-1.5 py-0.5 rounded border text-[10px] font-bold ${
                          getPeMoneyness(toNum(row.strike), underlyingLastPrice, atmStrike) === "ITM"
                            ? "bg-[linear-gradient(135deg,#ffe4e6_0%,#ffe4e6_48%,#f43f5e_49%,#f43f5e_52%,#fff1f2_53%,#fff1f2_100%)] border-rose-300 text-rose-900 dark:bg-[linear-gradient(135deg,#881337_0%,#881337_48%,#f43f5e_49%,#f43f5e_52%,#4c0519_53%,#4c0519_100%)] dark:border-rose-700 dark:text-rose-100"
                            : getPeMoneyness(toNum(row.strike), underlyingLastPrice, atmStrike) === "OTM"
                            ? "bg-[linear-gradient(135deg,#fff1f2_0%,#fff1f2_48%,#fb7185_49%,#fb7185_52%,#ffe4e6_53%,#ffe4e6_100%)] border-rose-200 text-rose-700 dark:bg-[linear-gradient(135deg,#4c0519_0%,#4c0519_48%,#fb7185_49%,#fb7185_52%,#881337_53%,#881337_100%)] dark:border-rose-800 dark:text-rose-300"
                            : "bg-amber-100 border-amber-300 text-amber-900 dark:bg-amber-900 dark:border-amber-700 dark:text-amber-100"
                        }`}
                      >
                        {getPeMoneyness(toNum(row.strike), underlyingLastPrice, atmStrike)}
                      </div>
                    </td>
                    <td className="px-2 py-1.5 tabular-nums text-rose-900 dark:text-rose-200">{formatIv(row.peLive?.implied_vol)}</td>
                    <td className="px-2 py-1.5 tabular-nums text-rose-800 dark:text-rose-300">{formatNum(row.peLive?.delta, 4)}</td>
                    <td className="px-2 py-1.5 tabular-nums text-rose-800 dark:text-rose-300">{formatNum(row.peLive?.gamma, 6)}</td>
                    <td className="px-2 py-1.5 tabular-nums text-rose-800 dark:text-rose-300">{formatNum(row.peLive?.theta, 4)}</td>
                    <td className="px-2 py-1.5 tabular-nums text-rose-800 dark:text-rose-300">{formatNum(row.peLive?.vega, 4)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
};

export default Options;
