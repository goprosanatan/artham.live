import React, { useState } from "react";

const MOCK_SUGGESTIONS = [
  {
    label: "With Options",
    children: ["Option Buying", "Option Selling", "Straddle", "Strangle"],
  },
  {
    label: "With Strategy",
    children: ["Swing", "Scalping", "BTST", "Intraday"],
  },
  { label: "NSE EQ" },
  { label: "Futures" },
];

export default function SmartSearchInput({className}) {
  const [query, setQuery] = useState("");
  const [selectedChips, setSelectedChips] = useState([]);

  const addChip = (chip) => {
    if (!selectedChips.some((c) => c.label === chip.label)) {
      setSelectedChips([...selectedChips, chip]);
    }
  };

  const removeChip = (chip) =>
    setSelectedChips(selectedChips.filter((c) => c.label !== chip.label));

  const filteredSuggestions = MOCK_SUGGESTIONS.filter((s) =>
    s.label.toLowerCase().includes(query.toLowerCase())
  );

  return (
    <div className={`${className} flex flex-row w-full max-w-2xl space-y-2 relative`}>
      {/* Input + Chips Row */}
      <div className="grow flex items-center gap-2 border border-gray-300 rounded-xl px-3 py-2 text-lg focus-within:ring-2 focus-within:ring-blue-500">
        {/* Main Input */}
        <input
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Search Scrip (Ex: Reliance)"
          className="flex-1 outline-none pr-2 text-base"
        />

        {/* Chips on Right */}
        <div className="flex flex-wrap gap-2 justify-end">
          {selectedChips.map((chip) => (
            <div
              key={chip.label}
              className="flex items-center bg-blue-600 text-white px-3 py-1 rounded-full text-sm"
            >
              {chip.label}
              <button
                onClick={() => removeChip(chip)}
                className="ml-2 font-bold"
              >
                ×
              </button>
            </div>
          ))}
        </div>
      </div>

      {/* Suggestion Chips */}
      {query.length > 0 && (
        <div className="grow flex flex-wrap gap-2 border border-gray-200 rounded-lg p-2 shadow-sm bg-white">
          {filteredSuggestions.map((s) => (
            <div key={s.label} className="relative group">
              <button
                className="px-3 py-1 border border-gray-400 rounded-full text-sm hover:bg-gray-100 transition"
                onClick={() => addChip(s)}
              >
                {s.label}
              </button>

              {/* Dropdown for children */}
              {s.children && (
                <div className="absolute left-0 top-full mt-1 hidden group-hover:block bg-white shadow-md rounded-md p-2 w-44 z-10">
                  {s.children.map((child) => (
                    <div
                      key={child}
                      className="px-2 py-1 text-sm hover:bg-gray-100 cursor-pointer rounded"
                      onClick={() => addChip({ label: child })}
                    >
                      {child}
                    </div>
                  ))}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
