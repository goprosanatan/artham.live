import { useState, useEffect } from "react";
import { useChartApi } from "@components/Chart/__API.js";

const InstrumentList = ({ onSelectInstrument, className = "" }) => {
  const { filterInstrument } = useChartApi();
  const [instruments, setInstruments] = useState({
    EQ: [],
    FUT: [],
    OPT: [],
  });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    loadInstruments();
  }, []);

  const loadInstruments = async () => {
    try {
      setLoading(true);
      setError(null);

      // Load Equity instruments (NSE + BSE)
      const eqNse = await filterInstrument("NSE", "NSE");
      const eqBse = await filterInstrument("BSE", "BSE");
      const relianceEq = [...eqNse, ...eqBse].filter(
        (i) => i.trading_symbol === "RELIANCE"
      );

      // Load Futures (NFO-FUT)
      const fut = await filterInstrument("NFO", "NFO-FUT");
      const relianceFut = fut.filter(
        (i) => i.underlying_trading_symbol === "RELIANCE"
      );

      // Load Options (NFO-OPT)
      const opt = await filterInstrument("NFO", "NFO-OPT");
      const relianceOpt = opt.filter(
        (i) => i.underlying_trading_symbol === "RELIANCE"
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

  const renderInstrumentsList = (list, section) => {
    if (list.length === 0) {
      return (
        <div className="p-2 text-xs text-gray-500 dark:text-gray-300 flex-shrink-0">
          No instruments
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
              {instrument.description}
            </div>
            <div className="text-xs text-gray-500 dark:text-gray-200 flex gap-2 items-center">
              <span className="px-1.5 py-0.5 bg-orange-100 text-orange-800 dark:bg-orange-800 dark:text-orange-50 rounded border border-orange-200 dark:border-orange-700">
                {instrument.exchange}
              </span>
              <span>ID: {instrument.instrument_id}</span>
            </div>
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
        <h2 className="font-bold text-gray-800 dark:text-gray-50">RELIANCE</h2>
        <p className="text-xs text-gray-600 dark:text-gray-200">
          {instruments.EQ.length +
            instruments.FUT.length +
            instruments.OPT.length}{" "}
          instruments
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
            {renderSection("EQ", "EQ", instruments.EQ)}
            {renderSection("FUTURES", "FUT", instruments.FUT)}
            {renderSection("OPTIONS", "OPT", instruments.OPT)}
          </div>
        )}
      </div>
    </div>
  );
};

export default InstrumentList;
