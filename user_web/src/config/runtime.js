const stripTrailingSlash = (value) => value.replace(/\/+$/, "");

const readEnv = (key) => {
  const value = import.meta.env[key];
  return typeof value === "string" ? value.trim() : "";
};

export const getApiAddress = () => {
  const explicitAddress = readEnv("VITE_API_ADDRESS");
  if (explicitAddress) return stripTrailingSlash(explicitAddress);

  const devPort = readEnv("VITE_API_DEV_PORT") || "8000";

  if (typeof window !== "undefined" && window.location) {
    if (import.meta.env.DEV) {
      const { protocol, hostname } = window.location;
      return `${protocol}//${hostname}:${devPort}`;
    }
    return window.location.origin;
  }

  return import.meta.env.DEV
    ? `http://localhost:${devPort}`
    : "http://localhost";
};

export const apiAddress = getApiAddress();
