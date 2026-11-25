const state = {
  apiBase: window.location.origin,
  tenantToken: "",
  traceToken: "",
  traceEndpoints: "",
  templateId: "",
  traceId: "",
  sandboxId: "",
  accessToken: "",
  ptySocket: null,
  ptyReady: false,
};

const elements = {
  connectionFeedback: document.querySelector("#connection-feedback"),
  connectionDot: document.querySelector("#connection-dot"),
  connectionState: document.querySelector("#connection-state"),
  connectionStateInline: document.querySelector("#connection-state-inline"),
  dashboardStatus: document.querySelector("#dashboard-status"),
  sandboxForm: document.querySelector("#sandbox-form"),
  createButton: document.querySelector("#create-button"),
  createState: document.querySelector("#create-state"),
  createFeedback: document.querySelector("#create-feedback"),
  tracePanel: document.querySelector("#trace"),
  traceIdentity: document.querySelector("#trace-identity"),
  traceWaterfall: document.querySelector("#trace-waterfall"),
  traceFooter: document.querySelector("#trace-footer"),
  refreshTrace: document.querySelector("#refresh-trace"),
  terminal: document.querySelector("#terminal"),
  terminalState: document.querySelector("#terminal-state"),
  terminalOutput: document.querySelector("#terminal-output"),
};

function updateConnectionState() {
  state.apiBase = window.location.origin;
}

function apiUrl(path) {
  return `${state.apiBase}${usingLandingProxy() ? "/api" : ""}${path}`;
}

function usingLandingProxy() {
  return new URL(state.apiBase, window.location.origin).origin === window.location.origin;
}

function authHeaders(token = state.tenantToken) {
  return token ? { Authorization: `Bearer ${token}` } : {};
}

function jsonHeaders(token = state.tenantToken) {
  return { ...authHeaders(token), "Content-Type": "application/json" };
}

async function readResponse(response) {
  const text = await response.text();
  if (!text) {
    return {};
  }
  try {
    return JSON.parse(text);
  } catch {
    return { detail: text };
  }
}

function responseMessage(payload, fallback) {
  return payload.detail || payload.message || payload.title || fallback;
}

function setFeedback(element, message, kind = "") {
  element.textContent = message;
  element.className = `form-feedback${kind ? ` is-${kind}` : ""}`;
}

function setConnectionState(kind) {
  elements.connectionDot.className = `connection-dot${kind ? ` is-${kind}` : ""}`;
  if (elements.connectionStateInline) {
    elements.connectionStateInline.textContent = kind === "live" ? "CONNECTED" : kind === "error" ? "OFFLINE" : "AUTO-CONNECT";
  }
}

function renderDashboard(payload) {
  const counts = payload.sandboxCounts || {};
  const running = counts.RUNNING || 0;
  const creating = counts.CREATING || 0;
  const suspended = counts.SUSPENDED || 0;
  elements.dashboardStatus.textContent = `${running} running · ${creating} creating · ${suspended} suspended`;
}

async function loadDashboard() {
  const response = await fetch(apiUrl("/v1/dashboard"), { headers: authHeaders() });
  const payload = await readResponse(response);
  if (!response.ok) {
    const error = new Error(responseMessage(payload, `Dashboard returned ${response.status}`));
    error.status = response.status;
    throw error;
  }
  renderDashboard(payload);
  return payload;
}

async function loadDefaultTemplate() {
  if (state.templateId) {
    return state.templateId;
  }
  const response = await fetch(apiUrl("/v1/templates"), { headers: authHeaders() });
  const payload = await readResponse(response);
  if (!response.ok) {
    throw new Error(responseMessage(payload, `Template lookup returned ${response.status}`));
  }
  const templates = Array.isArray(payload) ? payload : payload.templates || [];
  const readyTemplate = templates.find((template) => template.phase === "Ready" && template.templateId);
  if (!readyTemplate) {
    throw new Error("No Ready runtime template is available.");
  }
  state.templateId = readyTemplate.templateId;
  return state.templateId;
}

async function detectConnection() {
  updateConnectionState();
  try {
    const payload = await loadDashboard();
    await loadDefaultTemplate();
    setConnectionState("live");
    elements.connectionState.textContent = "CONNECTED";
    setFeedback(elements.connectionFeedback, `Connected · ${payload.sandboxes?.length || 0} sandboxes · Ready template selected`, "success");
  } catch (error) {
    if (error.status === 404) {
      try {
        const health = await fetch(apiUrl("/healthz"), { headers: authHeaders() });
        if (health.ok) {
          await loadDefaultTemplate();
          setConnectionState("live");
          elements.connectionState.textContent = "READY";
          elements.dashboardStatus.textContent = "API online";
          setFeedback(elements.connectionFeedback, "API reachable · Ready template selected", "success");
          return;
        }
      } catch {
        // Leave the initial connection result quiet until the user checks it.
      }
    }
    setConnectionState("error");
    elements.connectionState.textContent = "OFFLINE";
    elements.dashboardStatus.textContent = "OFFLINE";
    setFeedback(elements.connectionFeedback, "The configured cluster is unavailable.", "error");
  }
}

function setCreatePhase(phase, feedback = "") {
  elements.createState.textContent = phase;
  elements.sandboxForm.classList.toggle("is-creating", phase === "CREATING");
  elements.sandboxForm.classList.toggle("is-success", phase === "RUNNING");
  elements.sandboxForm.classList.toggle("is-error", phase === "FAILED");
  if (feedback) {
    setFeedback(elements.createFeedback, feedback);
  }
}

function idempotencyKey() {
  if (globalThis.crypto?.randomUUID) {
    return `landing-${crypto.randomUUID()}`;
  }
  return `landing-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function base64Url(value) {
  const bytes = new TextEncoder().encode(value);
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function setTerminalState(value) {
  elements.terminalState.textContent = value;
}

function terminalText(value) {
  return value
    .replace(/\u001b\][^\u0007]*(?:\u0007|\u001b\\)/g, "")
    .replace(/\u001b\[[0-?]*[ -/]*[@-~]/g, "")
    .replace(/\u001b[()][0-2A-Z]/g, "")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n");
}

function appendTerminalOutput(value) {
  const output = terminalText(value);
  if (!output) {
    return;
  }
  elements.terminalOutput.textContent += output;
  elements.terminalOutput.scrollTop = elements.terminalOutput.scrollHeight;
}

function closePty() {
  if (state.ptySocket) {
    state.ptySocket.close(1000, "replaced");
    state.ptySocket = null;
  }
  state.ptyReady = false;
  elements.terminalOutput.tabIndex = -1;
}

function resetTerminal() {
  closePty();
  state.accessToken = "";
  elements.terminal.hidden = true;
  setTerminalState("WAITING");
  elements.terminalOutput.textContent = "Opening a PTY after sandbox activation…";
  elements.terminalOutput.tabIndex = -1;
}

async function handlePtyMessage(socket, raw) {
  let value = raw;
  if (raw instanceof Blob) {
    value = await raw.text();
  } else if (raw instanceof ArrayBuffer) {
    value = new TextDecoder().decode(new Uint8Array(raw));
  }
  if (typeof value !== "string") {
    value = new TextDecoder().decode(value);
  }

  if (value.startsWith("{")) {
    try {
      const message = JSON.parse(value);
      if (message.type === "ready") {
        state.ptyReady = true;
        setTerminalState("CONNECTED");
        elements.terminalOutput.tabIndex = 0;
        elements.terminalOutput.focus();
        return;
      }
      if (message.type === "exit") {
        state.ptyReady = false;
        elements.terminalOutput.tabIndex = -1;
        setTerminalState(`EXIT ${message.exitCode ?? ""}`.trim());
        return;
      }
    } catch {
      // Shell output may begin with a brace and should remain visible.
    }
  }
  if (state.ptySocket === socket) {
    appendTerminalOutput(value);
  }
}

async function openPty() {
  closePty();
  elements.terminal.hidden = false;
  elements.terminal.classList.remove("is-live", "is-error");
  elements.terminal.classList.add("is-opening");
  elements.terminalOutput.textContent = "Connecting to the sandbox PTY…\n";
  setTerminalState("CONNECTING");

  const url = new URL("/api/pty", window.location.href);
  url.protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  url.searchParams.set("cmd", "/bin/sh");
  url.searchParams.set("args", "[]");
  url.searchParams.set("cols", "100");
  url.searchParams.set("rows", "28");
  const authProtocol = `kontinue-auth.${base64Url(state.sandboxId)}.${base64Url(state.accessToken)}`;
  const socket = new WebSocket(url, [authProtocol, "kontinue"]);
  state.ptySocket = socket;
  socket.binaryType = "arraybuffer";
  socket.onopen = () => {
    if (state.ptySocket === socket) {
      state.ptyReady = true;
      setTerminalState("CONNECTED");
      elements.terminal.classList.remove("is-opening", "is-error");
      elements.terminal.classList.add("is-live");
      elements.terminalOutput.textContent = "";
      elements.terminalOutput.tabIndex = 0;
      elements.terminalOutput.focus();
    }
  };
  socket.onmessage = (event) => {
    void handlePtyMessage(socket, event.data);
  };
  socket.onerror = () => {
    if (state.ptySocket === socket) {
      state.ptyReady = false;
      elements.terminalOutput.tabIndex = -1;
      setTerminalState("ERROR");
      elements.terminal.classList.remove("is-opening", "is-live");
      elements.terminal.classList.add("is-error");
      appendTerminalOutput("\nPTY connection failed.\n");
    }
  };
  socket.onclose = () => {
    if (state.ptySocket === socket) {
      state.ptyReady = false;
      state.ptySocket = null;
      elements.terminalOutput.tabIndex = -1;
      setTerminalState("CLOSED");
      elements.terminal.classList.remove("is-opening", "is-live");
    }
  };
}

function sendPtyInput(value) {
  if (state.ptyReady && state.ptySocket?.readyState === WebSocket.OPEN) {
    state.ptySocket.send(value);
  }
}

function terminalKeyData(event) {
  if (event.ctrlKey && event.key.length === 1) {
    const upper = event.key.toUpperCase();
    if (upper >= "@" && upper <= "_") {
      return String.fromCharCode(upper.charCodeAt(0) - 64);
    }
  }
  return {
    Enter: "\r",
    Backspace: "\x7f",
    Tab: "\t",
    Escape: "\x1b",
    ArrowUp: "\x1b[A",
    ArrowDown: "\x1b[B",
    ArrowRight: "\x1b[C",
    ArrowLeft: "\x1b[D",
    Home: "\x1b[H",
    End: "\x1b[F",
    Delete: "\x1b[3~",
  }[event.key] || (event.key.length === 1 ? event.key : "");
}

async function createSandbox(event) {
  event.preventDefault();
  updateConnectionState();
  elements.createButton.disabled = true;
  setCreatePhase("CREATING", "Persisting lifecycle intent and waiting for activation…");
  setFeedback(elements.createFeedback, "The API waits until the guest is ready.");
  clearTrace();

  try {
    await loadDefaultTemplate();
  } catch (error) {
    setCreatePhase("FAILED");
    setFeedback(elements.createFeedback, error.message, "error");
    showTraceError(error.message);
    elements.createButton.disabled = false;
    return;
  }

  const body = {
    templateId: state.templateId,
    idleTimeoutSeconds: 900,
    metadata: { source: "kontinue-landing", profile: "default" },
  };

  try {
    const response = await fetch(apiUrl("/v1/sandboxes"), {
      method: "POST",
      headers: { ...jsonHeaders(), "Idempotency-Key": idempotencyKey() },
      body: JSON.stringify(body),
    });
    const payload = await readResponse(response);
    if (!response.ok) {
      throw new Error(responseMessage(payload, `Sandbox creation returned ${response.status}`));
    }

    state.sandboxId = payload.sandboxId || "";
    state.accessToken = payload.accessToken || "";
    state.traceId = response.headers.get("X-Trace-ID") || "";
    setCreatePhase(payload.state || "RUNNING");
    setFeedback(elements.createFeedback, `${state.sandboxId} · ${payload.state || "RUNNING"}`, "success");
    await openPty();
    if (state.traceId || state.sandboxId) {
      await loadTrace();
    }
    await loadDashboard().catch(() => undefined);
  } catch (error) {
    setCreatePhase("FAILED");
    setFeedback(elements.createFeedback, error.message, "error");
    showTraceError(error.message);
  } finally {
    elements.createButton.disabled = false;
  }
}

function clearTrace() {
  state.traceId = "";
  state.sandboxId = "";
  elements.refreshTrace.disabled = true;
  elements.traceIdentity.innerHTML = '<span class="trace-placeholder">Waiting for the API response and its trace ID.</span>';
  elements.traceWaterfall.innerHTML = '<div class="empty-state"><span class="empty-mark">◌</span><p>Waiting for lifecycle spans.</p><span class="mono">POST /v1/sandboxes</span></div>';
  elements.traceFooter.firstElementChild.textContent = "The trace will be queried after activation completes.";
  elements.tracePanel.classList.remove("is-live", "is-error");
  resetTerminal();
}

function showTraceError(message) {
  elements.traceWaterfall.innerHTML = `<div class="empty-state"><span class="empty-mark trace-error-mark">!</span><p>Trace unavailable</p><span class="mono">${escapeHtml(message)}</span></div>`;
}

async function loadTrace() {
  const query = state.traceId ? `trace_id=${encodeURIComponent(state.traceId)}` : `sandbox_id=${encodeURIComponent(state.sandboxId)}`;
  const token = state.traceToken || state.tenantToken;
  elements.refreshTrace.disabled = true;
  elements.traceWaterfall.innerHTML = '<div class="empty-state"><span class="empty-mark">◌</span><p>Reading Flight Recorder.</p><span class="mono">GET /api/debug/traces</span></div>';
  try {
    const traceHeaders = { ...authHeaders(token) };
    if (state.traceEndpoints) {
      traceHeaders["X-Kontinue-Trace-Endpoints"] = state.traceEndpoints;
    }
    const tracePath = usingLandingProxy() ? `/api/trace?${query}` : `/debug/traces?${query}`;
    const response = await fetch(`${state.apiBase}${tracePath}`, { headers: traceHeaders });
    const payload = await readResponse(response);
    if (!response.ok) {
      throw new Error(responseMessage(payload, `Trace query returned ${response.status}`));
    }
    renderTrace(payload.spans || []);
    elements.tracePanel.classList.add("is-live");
    elements.refreshTrace.disabled = false;
  } catch (error) {
    elements.tracePanel.classList.add("is-error");
    showTraceError(error.message);
    elements.refreshTrace.disabled = false;
  }
}

function renderTrace(spans) {
  const sorted = [...spans].sort((a, b) => new Date(a.startTime) - new Date(b.startTime));
  const traceLabel = state.traceId || (sorted[0] && sorted[0].traceId) || "sandbox filter";
  elements.traceIdentity.innerHTML = `<span class="trace-id-label mono">TRACE</span><span class="trace-id mono">${escapeHtml(traceLabel)}</span><span class="trace-id-label mono">${sorted.length} spans</span>`;
  elements.traceFooter.firstElementChild.textContent = sorted.length ? "Spans returned by /debug/traces." : "No matching spans are in the active recorder buffer.";
  if (!sorted.length) {
    elements.traceWaterfall.innerHTML = '<div class="empty-state"><span class="empty-mark">∅</span><p>No spans found.</p><span class="mono">Try Refresh after the request completes.</span></div>';
    return;
  }

  const start = new Date(sorted[0].startTime).getTime();
  const end = Math.max(...sorted.map((span) => new Date(span.endTime).getTime()));
  const duration = Math.max(end - start, 1);
  const rows = sorted.map((span, index) => {
    const spanStart = new Date(span.startTime).getTime();
    const spanDuration = Math.max(Number(span.durationMs) || (new Date(span.endTime).getTime() - spanStart), 0);
    const left = ((spanStart - start) / duration) * 100;
    const width = Math.max((spanDuration / duration) * 100, 1.2);
    const status = span.status === "error" ? " is-error" : "";
    return `<div class="trace-row" style="--row-delay:${index * 45}ms"><div class="trace-service"><strong>${escapeHtml(span.service || "unknown")}</strong><span>${escapeHtml(span.name || "span")}</span></div><div class="trace-track"><span class="trace-bar${status}" style="left:${left}%;width:${width}%" title="${escapeHtml(formatDuration(spanDuration))}"></span></div><span class="trace-duration">${escapeHtml(formatDuration(spanDuration))}</span></div>`;
  });
  elements.traceWaterfall.innerHTML = `<div class="trace-scale"><span>0 ms</span><span>${formatDuration(duration)}</span></div>${rows.join("")}`;
}

function formatDuration(value) {
  if (value < 1) return `${value.toFixed(2)} ms`;
  if (value < 1000) return `${value.toFixed(1)} ms`;
  return `${(value / 1000).toFixed(2)} s`;
}

function escapeHtml(value) {
  return String(value).replace(/[&<>'"]/g, (character) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    "'": "&#39;",
    '"': "&quot;",
  })[character]);
}

elements.sandboxForm.addEventListener("submit", createSandbox);
elements.refreshTrace.addEventListener("click", loadTrace);
elements.terminalOutput.addEventListener("keydown", (event) => {
  const data = terminalKeyData(event);
  if (!data) {
    return;
  }
  event.preventDefault();
  sendPtyInput(data);
});
elements.terminalOutput.addEventListener("paste", (event) => {
  event.preventDefault();
  sendPtyInput(event.clipboardData?.getData("text") || "");
});
elements.terminalOutput.addEventListener("click", () => {
  if (state.ptyReady) {
    elements.terminalOutput.focus();
  }
});

const revealItems = document.querySelectorAll("[data-reveal]");
if ("IntersectionObserver" in window) {
  const revealObserver = new IntersectionObserver((entries, observer) => {
    for (const entry of entries) {
      if (!entry.isIntersecting) {
        continue;
      }
      entry.target.classList.add("is-visible");
      observer.unobserve(entry.target);
    }
  }, { threshold: 0.12 });
  revealItems.forEach((item) => revealObserver.observe(item));
} else {
  revealItems.forEach((item) => item.classList.add("is-visible"));
}

const navLinks = [...document.querySelectorAll("[data-nav-link]")];
const navSections = navLinks
  .map((link) => document.querySelector(link.getAttribute("href")))
  .filter(Boolean);
if ("IntersectionObserver" in window && navSections.length) {
  const navObserver = new IntersectionObserver((entries) => {
    for (const entry of entries) {
      if (!entry.isIntersecting) {
        continue;
      }
      navLinks.forEach((link) => link.classList.toggle("is-active", link.getAttribute("href") === `#${entry.target.id}`));
    }
  }, { rootMargin: "-35% 0px -55% 0px" });
  navSections.forEach((section) => navObserver.observe(section));
}

detectConnection();
