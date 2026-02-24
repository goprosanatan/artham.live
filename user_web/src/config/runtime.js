const stripTrailingSlash = (value) => value.replace(/\/+$/, "");

const readEnv = (key) => {
  const value = import.meta.env[key];
  return typeof value === "string" ? value.trim() : "";
};

const normalizeProtocol = (value, fallback) => {
  if (!value) return fallback;
  return value.endsWith(":") ? value : `${value}:`;
};

const buildAddress = ({ protocol, hostname, port }) => {
  return `${protocol}//${hostname}${port ? `:${port}` : ""}`;
};

export const getApiAddress = () => {
  const explicitAddress = readEnv("VITE_API_ADDRESS");
  if (explicitAddress) return stripTrailingSlash(explicitAddress);

  const isDev = import.meta.env.DEV;
  const devHost = readEnv("VITE_API_DEV_HOST");
  const devPort = readEnv("VITE_API_DEV_PORT") || "8000";
  const prodHost = readEnv("VITE_API_PROD_HOST");
  const prodPort = readEnv("VITE_API_PROD_PORT");
  const prodProtocol = readEnv("VITE_API_PROD_PROTOCOL");

  if (typeof window !== "undefined" && window.location) {
    const { protocol, hostname, origin } = window.location;

    if (isDev) {
      const targetHost = devHost || hostname;
      return buildAddress({
        protocol,
        hostname: targetHost,
        port: devPort,
      });
    }

    const useProdOverride = Boolean(prodHost || prodPort || prodProtocol);

    // Production mode:
    // - If VITE_API_PROD_* is set, use it.
    // - If page is HTTPS and only port override is provided, fallback to
    //   same-origin (reverse-proxy TLS setups).
    // - Else default to same-origin.
    if (useProdOverride) {
      const isHttpsPage = protocol === "https:";
      const hasOnlyPortOverride = !prodHost && !prodProtocol && Boolean(prodPort);
      if (isHttpsPage && hasOnlyPortOverride) {
        return origin;
      }

      return buildAddress({
        protocol: normalizeProtocol(prodProtocol, protocol),
        hostname: prodHost || hostname,
        port: prodPort,
      });
    }

    return origin;
  }

  if (isDev) {
    return buildAddress({
      protocol: "http:",
      hostname: devHost || "localhost",
      port: devPort,
    });
  }

  return buildAddress({
    protocol: normalizeProtocol(prodProtocol, "http:"),
    hostname: prodHost || "localhost",
    port: prodPort,
  });
};

export const apiAddress = getApiAddress();
