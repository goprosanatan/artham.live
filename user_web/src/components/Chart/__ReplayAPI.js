import { requestApi } from "@hooks/requestApi.js";

export function useReplayApi() {
  const { request } = requestApi();

  return {
    startSession: async (payload = {}) => {
      const data = await request("replay/session/start", "POST", {}, payload);
      return typeof data === "string" ? JSON.parse(data) : data;
    },
    listSessions: async () => {
      const data = await request("replay/session/list", "GET");
      const parsed = typeof data === "string" ? JSON.parse(data) : data;
      return Array.isArray(parsed?.sessions) ? parsed.sessions : [];
    },
    getSession: async (session_id) => {
      const data = await request(`replay/session/${session_id}`, "GET");
      return typeof data === "string" ? JSON.parse(data) : data;
    },
    controlSession: async (session_id, action) => {
      const data = await request(
        `replay/session/${session_id}/control`,
        "POST",
        {},
        { action }
      );
      return typeof data === "string" ? JSON.parse(data) : data;
    },
    deleteSession: async (session_id) => {
      const data = await request(
        `replay/session/${session_id}`,
        "DELETE"
      );
      return typeof data === "string" ? JSON.parse(data) : data;
    },
  };
}
