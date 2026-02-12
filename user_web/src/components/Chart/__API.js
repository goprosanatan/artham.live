import { requestApi } from "@hooks/requestApi.js";

export function useChartApi() {
  const { request } = requestApi(); // token-aware axios helper

  return {
    getExchangeAll: async () => {
      const data = await request("chart/exchange/all", "GET");
      // If backend returns a string → parse it
      return typeof data === "string" ? JSON.parse(data) : data;
    },
    getSegmentAll: async () => {
      const data = await request("chart/segment/all", "GET");
      // If backend returns a string → parse it
      return typeof data === "string" ? JSON.parse(data) : data;
    },
    getInstrumentAll: async () => {
      const data = await request("chart/instrument/all", "GET");
      // If backend returns a string → parse it
      return typeof data === "string" ? JSON.parse(data) : data;
    },
    filterInstrument: async (exchange, segment) => {
      const data = await request("chart/instrument/filter", "GET", {
        exchange,
        segment,
      });
      return typeof data === "string" ? JSON.parse(data) : data;
    },
    searchInstrument: async (exchange, segment, trading_symbol) => {
      const data = await request("chart/instrument/search", "GET", {
        exchange,
        segment,
        trading_symbol,
      });
      return typeof data === "string" ? JSON.parse(data) : data;
    },
    getInstrumentDetail: async (instrument_id) => {
      const data = await request("chart/instrument/detail", "GET", {
        instrument_id,
      });
      return typeof data === "string" ? JSON.parse(data) : data;
    },
    getData: async ({ instrument_id, timeframe, timestamp_end }) => {
      try {
        const raw = await request("chart/data/bars_slots", "GET", {
          instrument_id,
          timeframe,
          timestamp_end,
        });

        const payload = typeof raw === "string" ? JSON.parse(raw) : raw || {};

        const bars = Array.isArray(payload?.bars)
          ? payload.bars
          : Array.isArray(payload)
          ? payload
          : [];

        const slots = Array.isArray(payload?.slots) ? payload.slots : [];

        return { bars, slots };
      } catch (error) {
        console.error("getData error:", error);
        throw error;
      }
    },
  };
}


export function useOrderApi() {
  const { request } = requestApi();

  return {
    listBracket: async (limit = 100) => {
      const data = await request("order/list", "GET", { limit });
      return Array.isArray(data?.orders) ? data.orders : [];
    },
    submitBracket: async (payload) => {
      // send payload in request body, not as query params
      const data = await request("order/bracket", "POST", {}, payload);
      return data;
    },
    cancelBracket: async (bracket_id) => {
      const payload = { bracket_id };
      const data = await request("order/bracket", "DELETE", {}, payload);
      return data;
    },
    deleteBracket: async (bracket_id) => {
      const payload = { bracket_id };
      const data = await request("order/bracket/delete", "DELETE", {}, payload);
      return data;
    },
  };
}
