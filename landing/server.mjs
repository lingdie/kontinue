import { createReadStream, promises as fs } from "node:fs";
import { createServer } from "node:http";
import net from "node:net";
import { extname, join, normalize, sep } from "node:path";
import tls from "node:tls";
import { fileURLToPath } from "node:url";

const landingRoot = fileURLToPath(new URL("./public", import.meta.url));
const port = Number(process.env.PORT || 3000);
const upstream = new URL(process.env.KONTINUE_API_URL || "http://127.0.0.1:8081");
const gateway = new URL(process.env.KONTINUE_GATEWAY_URL || "http://127.0.0.1:18080");
const configuredTenantToken = process.env.KONTINUE_TENANT_TOKEN || "";
const configuredTraceToken = process.env.KONTINUE_TRACE_TOKEN || "";
const configuredTraceEndpoints = (process.env.KONTINUE_TRACE_ENDPOINTS || upstream.origin)
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean);

const contentTypes = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".yaml": "text/yaml; charset=utf-8",
};

function setSecurityHeaders(response) {
  response.setHeader("Cache-Control", "no-store");
  response.setHeader("X-Content-Type-Options", "nosniff");
  response.setHeader("Referrer-Policy", "same-origin");
}

function serveStatic(request, response) {
  const requestedPath = new URL(request.url, "http://landing.local").pathname;
  const requested = requestedPath === "/" ? "/index.html" : requestedPath;
  const candidate = normalize(join(landingRoot, requested));
  if (!candidate.startsWith(`${landingRoot}${sep}`)) {
    response.writeHead(400, { "Content-Type": "text/plain; charset=utf-8" });
    response.end("invalid path");
    return;
  }

  fs.stat(candidate)
    .then((file) => {
      if (!file.isFile()) {
        throw new Error("not a file");
      }
      setSecurityHeaders(response);
      response.writeHead(200, {
        "Content-Type": contentTypes[extname(candidate)] || "application/octet-stream",
      });
      createReadStream(candidate).pipe(response);
    })
    .catch(() => {
      response.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
      response.end("not found");
    });
}

function proxyApi(request, response) {
  const target = new URL(request.url.replace(/^\/api/, "") || "/", upstream);
  const headers = { ...request.headers, host: target.host };
  if (!headers.authorization) {
    const token = target.pathname === "/debug/traces" ? configuredTraceToken : configuredTenantToken;
    if (token) {
      headers.authorization = `Bearer ${token}`;
    }
  }
  const proxyRequest = fetch(target, {
    method: request.method,
    headers,
    body: ["GET", "HEAD"].includes(request.method) ? undefined : request,
    duplex: "half",
  });

  proxyRequest
    .then(async (upstreamResponse) => {
      setSecurityHeaders(response);
      response.writeHead(upstreamResponse.status, Object.fromEntries(upstreamResponse.headers));
      if (upstreamResponse.body) {
        for await (const chunk of upstreamResponse.body) {
          response.write(chunk);
        }
      }
      response.end();
    })
    .catch((error) => {
      response.writeHead(502, { "Content-Type": "application/json; charset=utf-8" });
      response.end(JSON.stringify({ title: "Cluster unavailable", detail: error.message }));
    });
}

function decodeWebSocketCredentials(header) {
  for (const offered of String(header || "").split(",")) {
    const parts = offered.trim().split(".");
    if (parts.length !== 3 || parts[0] !== "kontinue-auth") {
      continue;
    }
    try {
      return {
        sandboxId: Buffer.from(parts[1], "base64url").toString(),
        accessToken: Buffer.from(parts[2], "base64url").toString(),
      };
    } catch {
      return null;
    }
  }
  return null;
}

function upgradeError(socket, status, message) {
  const body = JSON.stringify({ error: message });
  socket.end(`HTTP/1.1 ${status} Error\r\nContent-Type: application/json\r\nContent-Length: ${Buffer.byteLength(body)}\r\nConnection: close\r\n\r\n${body}`);
}

function proxyPty(request, clientSocket, head) {
  const credentials = decodeWebSocketCredentials(request.headers["sec-websocket-protocol"]);
  if (!credentials?.sandboxId || !credentials.accessToken) {
    upgradeError(clientSocket, 401, "sandbox credentials required");
    return;
  }

  const connectOptions = {
    host: gateway.hostname,
    port: Number(gateway.port || (gateway.protocol === "https:" ? 443 : 80)),
  };
  const gatewaySocket = gateway.protocol === "https:"
    ? tls.connect(connectOptions)
    : net.connect(connectOptions);
  const upstreamPath = request.url.replace(/^\/api\/pty/, "/v1/pty");
  const upstreamRequest = [
    `GET ${upstreamPath} HTTP/1.1`,
    `Host: ${gateway.host}`,
    "Upgrade: websocket",
    "Connection: Upgrade",
    `Sec-WebSocket-Key: ${request.headers["sec-websocket-key"]}`,
    `Sec-WebSocket-Version: ${request.headers["sec-websocket-version"] || "13"}`,
    "Sec-WebSocket-Protocol: kontinue",
    `X-Kontinue-Sandbox-ID: ${credentials.sandboxId}`,
    `X-Kontinue-Token: ${credentials.accessToken}`,
    "",
    "",
  ].join("\r\n");

  let upstreamResponse = Buffer.alloc(0);
  let upgraded = false;
  clientSocket.pause();

  const closeBoth = () => {
    clientSocket.destroy();
    gatewaySocket.destroy();
  };

  gatewaySocket.on("connect", () => gatewaySocket.write(upstreamRequest));
  gatewaySocket.on("secureConnect", () => gatewaySocket.write(upstreamRequest));
  gatewaySocket.on("data", (chunk) => {
    if (upgraded) {
      clientSocket.write(chunk);
      return;
    }
    upstreamResponse = Buffer.concat([upstreamResponse, chunk]);
    const headerEnd = upstreamResponse.indexOf(Buffer.from("\r\n\r\n"));
    if (headerEnd < 0) {
      return;
    }

    let responseHead = upstreamResponse.subarray(0, headerEnd + 4).toString();
    if (responseHead.startsWith("HTTP/1.1 101") && !/\r\nsec-websocket-protocol:/i.test(responseHead)) {
      responseHead = responseHead.replace("\r\n\r\n", "\r\nSec-WebSocket-Protocol: kontinue\r\n\r\n");
    }
    clientSocket.write(responseHead);
    const remainder = upstreamResponse.subarray(headerEnd + 4);
    if (!responseHead.startsWith("HTTP/1.1 101")) {
      clientSocket.end(remainder);
      gatewaySocket.destroy();
      return;
    }

    upgraded = true;
    if (remainder.length) {
      clientSocket.write(remainder);
    }
    if (head.length) {
      gatewaySocket.write(head);
    }
    clientSocket.pipe(gatewaySocket);
    clientSocket.resume();
  });
  gatewaySocket.on("error", () => {
    if (!upgraded) {
      upgradeError(clientSocket, 502, "PTY gateway unavailable");
    }
    closeBoth();
  });
  gatewaySocket.on("close", () => clientSocket.destroy());
  clientSocket.on("error", () => gatewaySocket.destroy());
  clientSocket.on("close", () => gatewaySocket.destroy());
}

async function aggregateTrace(request, response) {
  const requestURL = new URL(request.url, "http://landing.local");
  const endpoints = (request.headers["x-kontinue-trace-endpoints"] || configuredTraceEndpoints.join(","))
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
  const authorization = request.headers.authorization || (configuredTraceToken ? `Bearer ${configuredTraceToken}` : "");
  const results = await Promise.allSettled(endpoints.map(async (endpoint) => {
    const traceURL = new URL("/debug/traces", endpoint);
    traceURL.search = requestURL.search;
    const traceResponse = await fetch(traceURL, {
      headers: authorization ? { Authorization: authorization } : {},
    });
    const payload = await traceResponse.json();
    if (!traceResponse.ok) {
      throw new Error(`${traceURL.origin} returned ${traceResponse.status}`);
    }
    return payload;
  }));

  const spans = new Map();
  const errors = [];
  for (const result of results) {
    if (result.status === "fulfilled") {
      for (const span of result.value.spans || []) {
        spans.set(`${span.service}:${span.spanId}`, span);
      }
    } else {
      errors.push(result.reason.message);
    }
  }

  setSecurityHeaders(response);
  response.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
  response.end(JSON.stringify({
    count: spans.size,
    spans: [...spans.values()].sort((a, b) => new Date(a.startTime) - new Date(b.startTime)),
    endpoints,
    errors,
  }));
}

const server = createServer((request, response) => {
  if (request.url.startsWith("/api/trace")) {
    aggregateTrace(request, response).catch((error) => {
      response.writeHead(502, { "Content-Type": "application/json; charset=utf-8" });
      response.end(JSON.stringify({ title: "Trace aggregation failed", detail: error.message }));
    });
    return;
  }
  if (request.url.startsWith("/api/")) {
    proxyApi(request, response);
    return;
  }
  if (request.method !== "GET" && request.method !== "HEAD") {
    response.writeHead(405, { "Content-Type": "text/plain; charset=utf-8" });
    response.end("method not allowed");
    return;
  }
  serveStatic(request, response);
});

server.on("upgrade", (request, socket, head) => {
  if (request.url.startsWith("/api/pty")) {
    proxyPty(request, socket, head);
    return;
  }
  socket.destroy();
});

server.listen(port, () => {
  console.log(`Kontinue landing page listening on http://127.0.0.1:${port}`);
  console.log(`Proxying /api/* to ${upstream.origin}`);
  console.log(`PTY gateway configured at ${gateway.origin}`);
});
