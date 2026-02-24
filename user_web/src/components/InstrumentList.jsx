import { useState, useEffect } from "react";
import { useChartApi } from "@components/Chart/__API.js";
import { useAuth } from "@contexts/authProvider.jsx";

const InstrumentList = ({
  onSelectInstrument,
  className = "",
  searchTerm = "",
  title = "RELIANCE",
}) => {
  const { searchInstrument } = useChartApi();
  const { token } = useAuth();
  const [instruments, setInstruments] = useState({
    EQ: [],
    FUT: [],
    OPT: [],
  });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const normalizedSearch = searchTerm.trim().toLowerCase();

  useEffect(() => {
    if (!token) return;
    loadInstruments();
  }, [token]);

  const loadInstruments = async () => {
    try {
      setLoading(true);
      setError(null);

      // Query only relevant instruments instead of loading full segments.
      const baseSymbol = (title || "RELIANCE").trim().toUpperCase();
      const matches = await searchInstrument("", "", baseSymbol, true);
      const list = Array.isArray(matches) ? matches : [];

      const relianceEq = list.filter(
        (i) =>
          i?.trading_symbol === baseSymbol &&
          ((i?.exchange === "NSE" && i?.segment === "NSE") ||
            (i?.exchange === "BSE" && i?.segment === "BSE"))
      );

      const relianceFut = list.filter(
        (i) =>
          i?.underlying_trading_symbol === baseSymbol &&
          i?.exchange === "NFO" &&
          i?.segment === "NFO-FUT"
      );

      const relianceOpt = list.filter(
        (i) =>
          i?.underlying_trading_symbol === baseSymbol &&
          i?.exchange === "NFO" &&
          i?.segment === "NFO-OPT"
      );

      setInstruments({
        EQ: relianceEq,
        FUT: relianceFut,
        OPT: relianceOpt,
      });
    } catch (err) {
      console.error("Failed to load instruments:", err);
      setError("Failed to load instruments");
    } finally {
      setLoading(false);
    }
  };

  const handleSelectInstrument = (instrument) => {
    if (onSelectInstrument) {
      onSelectInstrument(instrument);
    }
  };

  const filterBySearch = (list) => {
    if (!normalizedSearch) return list;
    return list.filter((instrument) => {
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
    });
  };

  const filtered = {
    EQ: filterBySearch(instruments.EQ),
    FUT: filterBySearch(instruments.FUT),
    OPT: filterBySearch(instruments.OPT),
  };
  const totalCount = filtered.EQ.length + filtered.FUT.length + filtered.OPT.length;
  const emptyLabel = normalizedSearch ? "No matches" : "No instruments";

  const renderInstrumentsList = (list, section) => {
    if (list.length === 0) {
      return (
        <div className="p-2 text-xs text-gray-500 dark:text-gray-300 flex-shrink-0">
          {emptyLabel}
        </div>
      );
    }

    return (
      <>
        {list.map((instrument) => (
          <div
            key={instrument.instrument_id}
            onClick={() => handleSelectInstrument(instrument)}
            className="p-2 border-b border-gray-200 dark:border-gray-600 cursor-pointer hover:bg-orange-100 dark:hover:bg-gray-700 transition-colors text-xs bg-white dark:bg-gray-800"
          >
            <div className="font-semibold text-gray-800 dark:text-gray-50">
              {instrument.trading_symbol || instrument.description}
            </div>
            <div className="text-xs text-gray-500 dark:text-gray-200 flex gap-2 items-center">
              <span className="px-1.5 py-0.5 bg-orange-100 text-orange-800 dark:bg-orange-800 dark:text-orange-50 rounded border border-orange-200 dark:border-orange-700">
                {instrument.exchange}
              </span>
              <span>ID: {instrument.instrument_id}</span>
            </div>
            {instrument.trading_symbol && instrument.description && (
              <div className="text-xs text-gray-500 dark:text-gray-200">
                {instrument.description}
              </div>
            )}
            {instrument.expiry && (
              <div className="text-xs text-gray-500 dark:text-gray-200">
                Exp: {new Date(instrument.expiry).toLocaleDateString()}
              </div>
            )}
            {instrument.segment === "NFO-OPT" && (
              <div className="text-xs text-gray-500 dark:text-gray-200">
                {instrument.strike} ({instrument.instrument_type})
              </div>
            )}
          </div>
        ))}
      </>
    );
  };

  const renderSection = (title, section, list) => (
    <div
      key={section}
      className="border-b border-black dark:border-gray-600 last:border-b-0 flex flex-col"
    >
      {/* Section Header */}
      <div className="px-3 py-2 bg-slate-200 dark:bg-gray-700 text-left font-semibold text-gray-800 dark:text-gray-50 text-sm flex-shrink-0 sticky top-0 z-10">
        <span>
          {title}
          <span className="ml-2 text-xs font-normal text-gray-600 dark:text-gray-300">
            ({list.length})
          </span>
        </span>
      </div>

      {/* Section Content */}
      {renderInstrumentsList(list, section)}
    </div>
  );

  if (error) {
    return (
      <div className="p-4 bg-red-100 text-red-700 rounded m-2">
        <p className="text-sm">{error}</p>
        <button
          onClick={loadInstruments}
          className="mt-2 px-3 py-1 bg-red-700 text-white rounded hover:bg-red-800 text-sm"
        >
          Retry
        </button>
      </div>
    );
  }

  return (
    <div
      className={`${className} flex flex-col overflow-hidden bg-white dark:bg-gray-900 rounded-lg border border-gray-300 dark:border-gray-700`}
    >
      {/* Header */}
      <div className="p-3 border-b border-gray-400 dark:border-gray-600 bg-gray-200 dark:bg-gray-800 flex-shrink-0">
        <h2 className="font-bold text-gray-800 dark:text-gray-50">{title}</h2>
        <p className="text-xs text-gray-600 dark:text-gray-200">
          {totalCount} instruments
        </p>
      </div>

      {/* Sections Container - Single scroll for all */}
      <div className="flex-1 overflow-y-auto min-h-0">
        {loading ? (
          <div className="flex items-center justify-center h-full">
            <div className="text-center">
              <div className="animate-spin rounded-full h-10 w-10 border-b-2 border-red-500 mx-auto mb-2"></div>
              <p className="text-sm text-gray-600">Loading...</p>
            </div>
          </div>
        ) : (
          <div className="flex flex-col">
            {renderSection("EQ", "EQ", filtered.EQ)}
            {renderSection("FUTURES", "FUT", filtered.FUT)}
            {renderSection("OPTIONS", "OPT", filtered.OPT)}
          </div>
        )}
      </div>
    </div>
  );
};

export default InstrumentList;
