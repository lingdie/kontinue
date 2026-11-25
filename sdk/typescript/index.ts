export class APIError extends Error {
  readonly status: number
  readonly code: string
  readonly detail: string

  constructor(status: number, code: string, detail: string) {
    super(`kontinue API ${status} ${code}: ${detail}`)
    this.name = "APIError"
    this.status = status
    this.code = code
    this.detail = detail
  }
}

export class CommandExitError extends Error {
  readonly result: CommandResult

  constructor(result: CommandResult) {
    super(`command exited with code ${result.exitCode}`)
    this.name = "CommandExitError"
    this.result = result
  }
}

export type SandboxInfo = {
  sandboxId: string
  templateId?: string
  activeSnapshotId?: string
  baseImage?: string
  tier?: string
  state: string
  executionId?: number
  lifecycleId?: string
  idleTimeoutSeconds?: number
  cpuMillis?: number
  memoryBytes?: number
  allowFilesystemOnlyFallback?: boolean
  accessToken?: string
  metadata?: Record<string, string>
  volumes?: Array<{ volumeId: string; mountPath: string; readOnly?: boolean }>
  [key: string]: unknown
}

export type ClientOptions = {
  baseUrl?: string
  apiUrl?: string
  gatewayUrl?: string
  tenantToken?: string
  apiKey?: string
  fetch?: typeof globalThis.fetch
  retries?: number
  timeoutMs?: number
  webSocket?: WebSocketConstructor
}

export type WebSocketLike = {
  readonly readyState: number
  binaryType: string
  onopen: (() => void) | null
  onmessage: ((event: { data: unknown }) => void) | null
  onerror: (() => void) | null
  onclose: (() => void) | null
  send(data: string | ArrayBuffer | Uint8Array): void
  close(code?: number, reason?: string): void
}

export type WebSocketConstructor = new (url: string, protocols?: string | string[]) => WebSocketLike

export type SandboxCreateOptions = {
  template?: string
  templateId?: string
  snapshotId?: string
  baseImage?: string
  tier?: string
  timeoutMs?: number
  idleTimeoutSeconds?: number
  envs?: Record<string, string>
  envVars?: Record<string, string>
  metadata?: Record<string, string>
  cpuMillis?: number
  memoryBytes?: number
  allowFilesystemOnlyFallback?: boolean
  volumes?: Array<{ volumeId: string; mountPath: string; readOnly?: boolean }>
}

export type CommandResult = {
  pid: number
  stdout: string
  stderr: string
  exitCode: number
  error?: string
  timedOut?: boolean
  durationMs?: number
}

export type ProcessInfo = {
  pid: number
  command: string[]
  startedAtUnixMs: number
  running: boolean
  exitCode?: number
}

export type FileInfo = {
  path: string
  isDir: boolean
  isFile: boolean
  size: number
  modifiedAtUnixMs?: number
}

export type FileEntry = {
  name: string
  path: string
  isDir: boolean
  isFile: boolean
  size: number
}

export type Webhook = {
  webhookId: string
  url: string
  events?: string[]
  secret?: string
  createdAt: string
  updatedAt: string
}

export type DashboardSummary = {
  generatedAt: string
  sandboxCounts: Record<string, number>
  sandboxes: SandboxInfo[]
  templates: unknown[]
  volumes: unknown[]
}

type FetchLike = typeof globalThis.fetch

function environment(name: string): string | undefined {
  const runtime = globalThis as typeof globalThis & { process?: { env?: Record<string, string | undefined> } }
  return runtime.process?.env?.[name]
}

function defaultClientOptions(): Required<Pick<ClientOptions, "baseUrl" | "gatewayUrl" | "tenantToken">> {
  return {
    baseUrl: environment("KONTINUE_API_URL") ?? environment("E2B_API_URL") ?? "http://127.0.0.1:8081",
    gatewayUrl: environment("KONTINUE_GATEWAY_URL") ?? environment("E2B_GATEWAY_URL") ?? "http://127.0.0.1:8080",
    tenantToken: environment("KONTINUE_TENANT_TOKEN") ?? environment("E2B_API_KEY") ?? "",
  }
}

function normalizeCreateOptions(templateOrOptions?: string | SandboxCreateOptions, options: SandboxCreateOptions = {}): Record<string, unknown> {
  const source: SandboxCreateOptions = typeof templateOrOptions === "string"
    ? { ...options, template: templateOrOptions }
    : { ...(templateOrOptions ?? {}), ...options }
  const payload: Record<string, unknown> = { ...source }
  const templateId = source.templateId ?? source.template
  const envVars = source.envVars ?? source.envs
  if (templateId) payload.templateId = templateId
  if (envVars) payload.envVars = envVars
  if (source.timeoutMs !== undefined) payload.idleTimeoutSeconds = Math.max(1, Math.ceil(source.timeoutMs / 1000))
  delete payload.template
  delete payload.templateId
  delete payload.envs
  delete payload.envVars
  delete payload.timeoutMs
  return {
    ...payload,
    ...(templateId ? { templateId } : {}),
    ...(envVars ? { envVars } : {}),
  }
}

export class Client {
  readonly baseUrl: string
  readonly gatewayUrl: string
  readonly tenantToken: string
  private readonly requestFetch: FetchLike
  private readonly retries: number
  private readonly timeoutMs?: number
  readonly webSocket?: WebSocketConstructor

  constructor(options: ClientOptions = {}) {
    const defaults = defaultClientOptions()
    this.baseUrl = (options.baseUrl ?? options.apiUrl ?? defaults.baseUrl).replace(/\/$/, "")
    this.gatewayUrl = (options.gatewayUrl ?? defaults.gatewayUrl).replace(/\/$/, "")
    this.tenantToken = options.tenantToken ?? options.apiKey ?? defaults.tenantToken
    this.requestFetch = options.fetch ?? globalThis.fetch.bind(globalThis)
    this.retries = options.retries ?? 3
    this.timeoutMs = options.timeoutMs
    const runtime = globalThis as typeof globalThis & { WebSocket?: WebSocketConstructor }
    this.webSocket = options.webSocket ?? runtime.WebSocket
  }

  async request<T>(method: string, path: string, body?: unknown): Promise<T> {
    return this.fetchJSON<T>(this.baseUrl, method, path, body, {
      ...(this.tenantToken ? { Authorization: `Bearer ${this.tenantToken}` } : {}),
    })
  }

  async gatewayRequest<T>(method: string, path: string, body?: unknown): Promise<T> {
    return this.fetchJSON<T>(this.gatewayUrl, method, path, body, {
      ...(this.tenantToken ? { Authorization: `Bearer ${this.tenantToken}` } : {}),
    })
  }

  async sandboxRequest<T>(sandboxId: string, accessToken: string, method: string, path: string, body?: unknown): Promise<T> {
    return this.fetchJSON<T>(this.gatewayUrl, method, path, body, {
      "X-Kontinue-Sandbox-ID": sandboxId,
      "X-Kontinue-Token": accessToken,
    })
  }

  async gatewayBytes(method: string, path: string, body?: Uint8Array): Promise<Uint8Array> {
    const response = await this.requestFetch(this.gatewayUrl + path, {
      method,
      headers: {
        ...(this.tenantToken ? { Authorization: `Bearer ${this.tenantToken}` } : {}),
        ...(body ? { "Content-Type": "application/octet-stream" } : {}),
      },
      body: body as BodyInit | undefined,
    })
    const raw = new Uint8Array(await response.arrayBuffer())
    if (!response.ok) throw this.errorFromResponse(response.status, raw)
    return raw
  }

  async sandboxBytes(sandboxId: string, accessToken: string, method: string, path: string, body?: Uint8Array): Promise<Uint8Array> {
    const response = await this.requestFetch(this.gatewayUrl + path, {
      method,
      headers: {
        "X-Kontinue-Sandbox-ID": sandboxId,
        "X-Kontinue-Token": accessToken,
        ...(body ? { "Content-Type": "application/octet-stream" } : {}),
      },
      body: body as BodyInit | undefined,
    })
    const raw = new Uint8Array(await response.arrayBuffer())
    if (!response.ok) throw this.errorFromResponse(response.status, raw)
    return raw
  }

  async *sandboxStream(sandboxId: string, accessToken: string, path: string): AsyncGenerator<Record<string, unknown>> {
    const response = await this.requestFetch(this.gatewayUrl + path, {
      method: "GET",
      headers: { "X-Kontinue-Sandbox-ID": sandboxId, "X-Kontinue-Token": accessToken },
    })
    if (!response.ok) {
      const raw = await response.text()
      throw this.errorFromResponse(response.status, raw)
    }
    if (!response.body) throw new Error("sandbox stream response has no body")
    const reader = response.body.getReader()
    const decoder = new TextDecoder()
    let buffer = ""
    for (;;) {
      const chunk = await reader.read()
      buffer += decoder.decode(chunk.value ?? new Uint8Array(), { stream: !chunk.done })
      const lines = buffer.split("\n")
      buffer = lines.pop() ?? ""
      for (const line of lines) {
        if (line.trim()) yield JSON.parse(line) as Record<string, unknown>
      }
      if (chunk.done) break
    }
    if (buffer.trim()) yield JSON.parse(buffer) as Record<string, unknown>
  }

  private async fetchJSON<T>(origin: string, method: string, path: string, body: unknown, extraHeaders: Record<string, string>): Promise<T> {
    for (let attempt = 0; ; attempt += 1) {
      const controller = this.timeoutMs ? new AbortController() : undefined
      const timeout = controller ? setTimeout(() => controller.abort(), this.timeoutMs) : undefined
      try {
        const response = await this.requestFetch(origin + path, {
          method,
          headers: { "Content-Type": "application/json", ...extraHeaders },
          body: body === undefined ? undefined : JSON.stringify(body),
          signal: controller?.signal,
        })
        const raw = await response.text()
        if (response.status >= 500 && attempt < this.retries) {
          await new Promise((resolve) => setTimeout(resolve, 100 * 2 ** Math.min(attempt, 5)))
          continue
        }
        if (!response.ok) throw this.errorFromResponse(response.status, raw)
        return (raw ? JSON.parse(raw) : undefined) as T
      } finally {
        if (timeout) clearTimeout(timeout)
      }
    }
  }

  private errorFromResponse(status: number, raw: string | Uint8Array): APIError {
    const text = typeof raw === "string" ? raw : new TextDecoder().decode(raw)
    let problem: { code?: string; error?: string } = {}
    try { problem = JSON.parse(text) } catch { /* preserve the response body in detail */ }
    return new APIError(status, problem.code ?? "", problem.error ?? text)
  }

  createSandbox(options: Record<string, unknown> = {}) { return this.request<SandboxInfo>("POST", "/v1/sandboxes", options) }
  listSandboxes(options: { state?: string; templateId?: string; cursor?: string; limit?: number } = {}) {
    const query = new URLSearchParams()
    for (const [key, value] of Object.entries({ state: options.state, templateId: options.templateId, cursor: options.cursor, limit: options.limit })) {
      if (value !== undefined && value !== "") query.set(key, String(value))
    }
    if (!query.has("limit")) query.set("limit", "100")
    return this.request<{ sandboxes: SandboxInfo[]; nextCursor: string }>("GET", `/v1/sandboxes?${query}`)
  }
  connect(sandboxId: string) { return this.request<SandboxInfo>("POST", `/v1/sandboxes/${encodeURIComponent(sandboxId)}/connect`) }
  suspend(sandboxId: string) { return this.request<unknown>("POST", `/v1/sandboxes/${encodeURIComponent(sandboxId)}/suspend`) }
  resume(sandboxId: string) { return this.request<SandboxInfo>("POST", `/v1/sandboxes/${encodeURIComponent(sandboxId)}/resume`) }
  snapshot(sandboxId: string) { return this.request<unknown>("POST", `/v1/sandboxes/${encodeURIComponent(sandboxId)}/snapshots`) }
  deleteSandbox(sandboxId: string) { return this.request<void>("DELETE", `/v1/sandboxes/${encodeURIComponent(sandboxId)}`) }
  setSandboxTimeout(sandboxId: string, timeoutMs: number) {
    return this.request<SandboxInfo>("POST", `/v1/sandboxes/${encodeURIComponent(sandboxId)}/timeout`, { idleTimeoutSeconds: Math.max(1, Math.ceil(timeoutMs / 1000)) })
  }
  fork(sandboxId: string, options: { count?: number; envVars?: Record<string, string>; children?: unknown[] } = {}) {
    return this.request<unknown[]>("POST", `/v1/sandboxes/${encodeURIComponent(sandboxId)}/fork`, options)
  }
  signedFileUrl(sandboxId: string, options: { path: string; operation: "read" | "write" | "download" | "upload"; expiresInSeconds?: number }) {
    return this.request<{ url: string; path: string; operation: string; expiresAtUnixSeconds: number }>("POST", `/v1/sandboxes/${encodeURIComponent(sandboxId)}/files/urls`, options)
  }
  createVolume(options: Record<string, unknown> = {}) { return this.request<unknown>("POST", "/v1/volumes", options) }
  listVolumes() { return this.request<unknown[]>("GET", "/v1/volumes") }
  listTemplates() { return this.request<unknown[]>("GET", "/v1/templates") }
  createTemplate(options: Record<string, unknown>) { return this.request<unknown>("POST", "/v1/templates", options) }
  getTemplate(templateId: string) { return this.request<unknown>("GET", `/v1/templates/${encodeURIComponent(templateId)}`) }
  rebuildTemplate(templateId: string, options: Record<string, unknown> = {}) { return this.request<unknown>("POST", `/v1/templates/${encodeURIComponent(templateId)}/builds`, options) }
  templateBuilds(templateId: string) { return this.request<unknown[]>("GET", `/v1/templates/${encodeURIComponent(templateId)}/builds`) }
  templateLogs(templateId: string) { return this.request<unknown>("GET", `/v1/templates/${encodeURIComponent(templateId)}/logs`) }
  tagTemplate(templateId: string, tag: string, snapshotId?: string) { return this.request<unknown>("POST", `/v1/templates/${encodeURIComponent(templateId)}/tags`, { tag, ...(snapshotId ? { snapshotId } : {}) }) }
  deleteTemplate(templateId: string) { return this.request<void>("DELETE", `/v1/templates/${encodeURIComponent(templateId)}`) }
  createWebhook(options: { url: string; events?: string[]; secret?: string }) { return this.request<Webhook>("POST", "/v1/webhooks", options) }
  listWebhooks() { return this.request<Webhook[]>("GET", "/v1/webhooks") }
  getWebhook(webhookId: string) { return this.request<Webhook>("GET", `/v1/webhooks/${encodeURIComponent(webhookId)}`) }
  deleteWebhook(webhookId: string) { return this.request<void>("DELETE", `/v1/webhooks/${encodeURIComponent(webhookId)}`) }
  dashboard() { return this.request<DashboardSummary>("GET", "/v1/dashboard") }
}

export type CommandRunOptions = {
  background?: boolean
  cwd?: string
  envs?: Record<string, string>
  env?: Record<string, string>
  timeoutMs?: number
  stdin?: string
  onStdout?: (chunk: string) => void
  onStderr?: (chunk: string) => void
  raiseOnError?: boolean
}

export class CommandHandle {
  private readonly commands: Commands
  readonly pid: number

  constructor(commands: Commands, pid: number) {
    this.commands = commands
    this.pid = pid
  }

  async wait(): Promise<CommandResult> {
    const result = await this.commands.wait(this.pid)
    if (result.exitCode !== 0) throw new CommandExitError(result)
    return result
  }

  async kill(): Promise<void> {
    await this.commands.kill(this.pid)
  }
}

export class Commands {
  private readonly client: Client
  private readonly sandbox: Sandbox

  constructor(client: Client, sandbox: Sandbox) {
    this.client = client
    this.sandbox = sandbox
  }

  async run(command: string, options: CommandRunOptions = {}): Promise<CommandResult | CommandHandle> {
    const streamOutput = !options.background && (options.onStdout !== undefined || options.onStderr !== undefined)
    const result = await this.client.sandboxRequest<CommandResult>(this.sandbox.sandboxId, this.sandbox.accessToken, "POST", "/v1/process/exec", {
      command: ["/bin/sh", "-c", command],
      workingDir: options.cwd,
      env: options.envs ?? options.env,
      stdin: options.stdin,
      timeoutMs: options.timeoutMs,
      background: options.background || streamOutput,
    })
    if (options.background) return new CommandHandle(this, result.pid)
    if (streamOutput) {
      let stdout = ""
      let stderr = ""
      let exitCode = result.exitCode
      for await (const event of this.client.sandboxStream(this.sandbox.sandboxId, this.sandbox.accessToken, `/v1/process/logs/stream?pid=${encodeURIComponent(result.pid)}`)) {
        if (event.type === "stdout") {
          const chunk = String(event.data ?? "")
          stdout += chunk
          options.onStdout?.(chunk)
        } else if (event.type === "stderr") {
          const chunk = String(event.data ?? "")
          stderr += chunk
          options.onStderr?.(chunk)
        } else if (event.type === "exit") {
          exitCode = Number(event.exitCode ?? 0)
        }
      }
      const streamedResult = { ...result, stdout, stderr, exitCode }
      if ((options.raiseOnError ?? true) && streamedResult.exitCode !== 0) throw new CommandExitError(streamedResult)
      return streamedResult
    }
    options.onStdout?.(result.stdout)
    options.onStderr?.(result.stderr)
    if ((options.raiseOnError ?? true) && result.exitCode !== 0) throw new CommandExitError(result)
    return result
  }

  list(): Promise<ProcessInfo[]> {
    return this.client.sandboxRequest<ProcessInfo[]>(this.sandbox.sandboxId, this.sandbox.accessToken, "GET", "/v1/process/list")
  }

  connect(pid: number): CommandHandle {
    return new CommandHandle(this, pid)
  }

  async kill(pid: number): Promise<void> {
    await this.client.sandboxRequest<void>(this.sandbox.sandboxId, this.sandbox.accessToken, "POST", `/v1/process/kill?pid=${encodeURIComponent(pid)}`)
  }

  async wait(pid: number, pollIntervalMs = 100): Promise<CommandResult> {
    for (;;) {
      const result = await this.client.sandboxRequest<CommandResult & { running?: boolean }>(this.sandbox.sandboxId, this.sandbox.accessToken, "GET", `/v1/process/logs?pid=${encodeURIComponent(pid)}`)
      if (!result.running) return result
      await new Promise((resolve) => setTimeout(resolve, pollIntervalMs))
    }
  }
}

export type FileReadOptions = { format?: "text" | "bytes" }

export class Filesystem {
  private readonly client: Client
  private readonly sandbox: Sandbox

  constructor(client: Client, sandbox: Sandbox) {
    this.client = client
    this.sandbox = sandbox
  }

  async read(path: string, options: FileReadOptions = {}): Promise<string | Uint8Array> {
    const bytes = await this.client.sandboxBytes(this.sandbox.sandboxId, this.sandbox.accessToken, "GET", `/v1/files?path=${encodeURIComponent(path)}`)
    return options.format === "bytes" ? bytes : new TextDecoder().decode(bytes)
  }

  async write(path: string, data: string | Uint8Array, options: { mode?: number } = {}): Promise<void> {
    if (options.mode !== undefined) {
      await this.client.sandboxRequest<void>(this.sandbox.sandboxId, this.sandbox.accessToken, "PUT", "/v1/files", { path, content: Array.from(typeof data === "string" ? new TextEncoder().encode(data) : data), mode: options.mode })
      return
    }
    const bytes = typeof data === "string" ? new TextEncoder().encode(data) : data
    await this.client.sandboxBytes(this.sandbox.sandboxId, this.sandbox.accessToken, "PUT", `/v1/files/stream?path=${encodeURIComponent(path)}`, bytes)
  }

  async downloadUrl(path: string, expiresInSeconds = 300): Promise<string> {
    const result = await this.client.signedFileUrl(this.sandbox.sandboxId, { path, operation: "download", expiresInSeconds })
    return result.url
  }

  async uploadUrl(path: string, expiresInSeconds = 300): Promise<string> {
    const result = await this.client.signedFileUrl(this.sandbox.sandboxId, { path, operation: "upload", expiresInSeconds })
    return result.url
  }

  async *watch(path: string): AsyncGenerator<FileInfo> {
    for await (const event of this.client.sandboxStream(this.sandbox.sandboxId, this.sandbox.accessToken, `/v1/files/watch?path=${encodeURIComponent(path)}`)) {
      if (event.type === "change" && event.file) yield event.file as FileInfo
    }
  }

  list(path = "."): Promise<FileEntry[]> {
    return this.client.sandboxRequest<FileEntry[]>(this.sandbox.sandboxId, this.sandbox.accessToken, "GET", `/v1/files/list?path=${encodeURIComponent(path)}`)
  }

  getInfo(path: string): Promise<FileInfo> {
    return this.client.sandboxRequest<FileInfo>(this.sandbox.sandboxId, this.sandbox.accessToken, "GET", `/v1/files/info?path=${encodeURIComponent(path)}`)
  }

  async exists(path: string): Promise<boolean> {
    try {
      await this.getInfo(path)
      return true
    } catch (error) {
      if (error instanceof APIError && error.status === 404) return false
      throw error
    }
  }

  async makeDir(path: string): Promise<void> {
    await this.client.sandboxRequest<void>(this.sandbox.sandboxId, this.sandbox.accessToken, "POST", `/v1/files/mkdir?path=${encodeURIComponent(path)}`)
  }

  async remove(path: string): Promise<void> {
    await this.client.sandboxRequest<void>(this.sandbox.sandboxId, this.sandbox.accessToken, "DELETE", `/v1/files?path=${encodeURIComponent(path)}`)
  }

  async rename(from: string, to: string): Promise<void> {
    await this.client.sandboxRequest<void>(this.sandbox.sandboxId, this.sandbox.accessToken, "POST", "/v1/files/rename", { from, to })
  }
}

export type PtyCreateOptions = {
  cmd?: string
  args?: string[]
  command?: string
  envs?: Record<string, string>
  cwd?: string
  cols?: number
  rows?: number
  timeoutMs?: number
  onData?: (data: string | Uint8Array) => void
  onExit?: (exitCode?: number) => void
}

function base64Url(value: string): string {
  const bytes = new TextEncoder().encode(value)
  let binary = ""
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "")
}

function ptyWebSocketURL(baseUrl: string, sandboxId: string, accessToken: string, options: PtyCreateOptions, pid?: number): { url: string; protocols: string[] } {
  const url = new URL("/v1/pty", baseUrl)
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:"
  if (pid !== undefined) {
    url.searchParams.set("pid", String(pid))
  } else {
    const command = options.command
      ? { cmd: "/bin/sh", args: ["-c", options.command] }
      : { cmd: options.cmd ?? "/bin/sh", args: options.args ?? [] }
    url.searchParams.set("cmd", command.cmd)
    url.searchParams.set("args", JSON.stringify(command.args))
    url.searchParams.set("cols", String(options.cols ?? 80))
    url.searchParams.set("rows", String(options.rows ?? 24))
    if (options.cwd) url.searchParams.set("cwd", options.cwd)
    if (options.envs) url.searchParams.set("envs", JSON.stringify(options.envs))
    if (options.timeoutMs !== undefined) url.searchParams.set("timeoutMs", String(options.timeoutMs))
  }
  const authProtocol = `kontinue-auth.${base64Url(sandboxId)}.${base64Url(accessToken)}`
  return { url: url.toString(), protocols: [authProtocol, "kontinue"] }
}

export class PtySession {
  pid: number
  private readonly socket: WebSocketLike
  private readonly options: PtyCreateOptions
  private readyResolve!: (pid: number) => void
  private readyReject!: (error: Error) => void
  private readonly readyPromise: Promise<number>
  private readonly closedPromise: Promise<number | undefined>
  private closedResolve!: (exitCode?: number) => void
  private sawReady = false
  private closed = false
  private exitNotified = false
  private exitCode?: number

  constructor(socket: WebSocketLike, options: PtyCreateOptions = {}) {
    this.socket = socket
    this.options = options
    this.pid = 0
    this.readyPromise = new Promise((resolve, reject) => {
      this.readyResolve = resolve
      this.readyReject = reject
    })
    this.closedPromise = new Promise((resolve) => { this.closedResolve = resolve })
    socket.binaryType = "arraybuffer"
    socket.onmessage = (event) => { void this.handleMessage(event.data) }
    socket.onerror = () => {
      const error = new Error("PTY WebSocket failed")
      if (!this.sawReady) this.readyReject(error)
    }
    socket.onclose = () => {
      if (!this.closed) {
        this.closed = true
        this.closedResolve(this.exitCode)
        this.notifyExit()
      }
    }
  }

  get ready(): Promise<number> {
    return this.readyPromise
  }

  get done(): Promise<number | undefined> {
    return this.closedPromise
  }

  get isClosed(): boolean {
    return this.closed
  }

  async wait(): Promise<number | undefined> {
    return this.done
  }

  sendInput(data: string | Uint8Array): void {
    this.socket.send(data)
  }

  resize(cols: number, rows: number): void {
    this.socket.send(JSON.stringify({ type: "resize", cols, rows }))
  }

  kill(): void {
    this.socket.send(JSON.stringify({ type: "kill" }))
    this.socket.close(1000, "killed")
  }

  close(): void {
    this.socket.close(1000, "closed")
  }

  private async handleMessage(raw: unknown): Promise<void> {
    const data = raw instanceof ArrayBuffer
      ? new Uint8Array(raw)
      : raw instanceof Uint8Array
        ? raw
        : raw instanceof Blob
          ? new Uint8Array(await raw.arrayBuffer())
          : String(raw)
    const text = typeof data === "string" ? data : new TextDecoder().decode(data)
    if (typeof data === "string" || text.startsWith("{")) {
      try {
        const message = JSON.parse(text) as { type?: string; pid?: number; exitCode?: number }
        if (message.type === "ready" && message.pid) {
          this.sawReady = true
          this.pid = message.pid
          this.readyResolve(message.pid)
          return
        }
        if (message.type === "exit") {
          this.exitCode = message.exitCode
          this.notifyExit()
          return
        }
      } catch {
        // Terminal output is allowed to contain arbitrary text.
      }
    }
    this.options.onData?.(data)
  }

  private notifyExit(): void {
    if (this.exitNotified) return
    this.exitNotified = true
    this.options.onExit?.(this.exitCode)
  }
}

export class Pty {
  private readonly sessions = new Map<number, PtySession>()
  private readonly client: Client
  private readonly sandbox: Sandbox

  constructor(client: Client, sandbox: Sandbox) {
    this.client = client
    this.sandbox = sandbox
  }

  async create(options: PtyCreateOptions = {}): Promise<PtySession> {
    return this.open(options)
  }

  private async open(options: PtyCreateOptions, pid?: number): Promise<PtySession> {
    if (!this.client.webSocket) throw new Error("PTY requires a WebSocket implementation in ClientOptions.webSocket or globalThis.WebSocket")
    const connection = ptyWebSocketURL(this.client.gatewayUrl, this.sandbox.sandboxId, this.sandbox.accessToken, options, pid)
    const socket = new this.client.webSocket(connection.url, connection.protocols)
    const session = new PtySession(socket, options)
    const socketError = socket.onerror
    await new Promise<void>((resolve, reject) => {
      socket.onopen = () => resolve()
      socket.onerror = () => {
        socketError?.()
        reject(new Error("PTY WebSocket failed to open"))
      }
    })
    const sessionPid = await session.ready
    this.sessions.set(sessionPid, session)
    return session
  }

  async run(command: string, options: PtyCreateOptions = {}): Promise<PtySession> {
    return this.create({ ...options, command })
  }

  async connect(pid: number, options: PtyCreateOptions = {}): Promise<PtySession> {
    const session = this.sessions.get(pid)
    if (session && !session.isClosed) return session
    return this.open(options, pid)
  }

  async sendInput(pid: number, data: string | Uint8Array): Promise<void> {
    const session = await this.connect(pid)
    session.sendInput(data)
  }

  async resize(pid: number, size: { cols: number; rows: number }): Promise<void> {
    const session = await this.connect(pid)
    session.resize(size.cols, size.rows)
  }

  async kill(pid: number): Promise<void> {
    const session = await this.connect(pid)
    session.kill()
    this.sessions.delete(pid)
  }
}

export class Sandbox {
  readonly commands: Commands
  readonly files: Filesystem
  readonly pty: Pty
  readonly client: Client
  private info: SandboxInfo

  constructor(client: Client, info: SandboxInfo) {
    this.client = client
    this.info = info
    this.commands = new Commands(client, this)
    this.files = new Filesystem(client, this)
    this.pty = new Pty(client, this)
  }

  get sandboxId() { return this.info.sandboxId }
  get sandboxDomain() { return this.getHost(49983) }
  get accessToken() { return this.info.accessToken ?? "" }
  get state() { return this.info.state }
  get templateId() { return this.info.templateId }
  get metadata() { return this.info.metadata }
  get infoSnapshot() { return { ...this.info } }

  static async create(templateOrOptions?: string | SandboxCreateOptions, options: SandboxCreateOptions & { clientOptions?: ClientOptions } = {}): Promise<Sandbox> {
    const client = options.clientOptions ? new Client(options.clientOptions) : new Client()
    const { clientOptions: _clientOptions, ...createOptions } = options
    const info = await client.request<SandboxInfo>("POST", "/v1/sandboxes", normalizeCreateOptions(templateOrOptions, createOptions))
    return new Sandbox(client, info)
  }

  static async connect(sandboxId: string, options: ClientOptions = {}): Promise<Sandbox> {
    const client = new Client(options)
    const info = await client.connect(sandboxId)
    return new Sandbox(client, info)
  }

  static async list(options: ClientOptions & { state?: string; templateId?: string; cursor?: string; limit?: number } = {}): Promise<{ sandboxes: Sandbox[]; nextCursor: string }> {
    const { state, templateId, cursor, limit, ...clientOptions } = options
    const client = new Client(clientOptions)
    const page = await client.listSandboxes({ state, templateId, cursor, limit })
    return { sandboxes: page.sandboxes.map((info) => new Sandbox(client, info)), nextCursor: page.nextCursor }
  }

  async connect(): Promise<this> {
    const next = await this.client.connect(this.sandboxId)
    this.info = { ...this.info, ...next, accessToken: next.accessToken ?? this.accessToken }
    return this
  }

  async getInfo(): Promise<SandboxInfo> {
    const next = await this.client.request<SandboxInfo>("GET", `/v1/sandboxes/${encodeURIComponent(this.sandboxId)}`)
    this.info = { ...this.info, ...next, accessToken: next.accessToken ?? this.accessToken }
    return this.infoSnapshot
  }

  async kill(): Promise<void> {
    await this.client.deleteSandbox(this.sandboxId)
  }

  async setTimeout(timeoutMs: number): Promise<void> {
    const next = await this.client.setSandboxTimeout(this.sandboxId, timeoutMs)
    this.info = { ...this.info, ...next, accessToken: next.accessToken ?? this.accessToken }
  }

  async pause(): Promise<void> {
    await this.client.suspend(this.sandboxId)
  }

  async betaPause(): Promise<void> {
    await this.pause()
  }

  async resume(): Promise<this> {
    const next = await this.client.resume(this.sandboxId)
    this.info = { ...this.info, ...next, accessToken: next.accessToken ?? this.accessToken }
    return this
  }

  getHost(port = 49983): string {
    const base = this.client.gatewayUrl
    const parsed = new URL(base)
    const domain = parsed.hostname
    if (domain !== "127.0.0.1" && domain !== "localhost") {
      parsed.hostname = `${port}-${this.sandboxId}.${domain}`
      return parsed.toString().replace(/\/$/, "")
    }
    return `${base}/${port}-${encodeURIComponent(this.sandboxId)}`
  }

}

export class SandboxResource {
  private readonly client: Client

  constructor(client: Client) {
    this.client = client
  }
  async create(templateOrOptions?: string | SandboxCreateOptions, options: SandboxCreateOptions = {}) {
    const info = await this.client.request<SandboxInfo>("POST", "/v1/sandboxes", normalizeCreateOptions(templateOrOptions, options))
    return new Sandbox(this.client, info)
  }
  async connect(sandboxId: string) {
    return new Sandbox(this.client, await this.client.connect(sandboxId))
  }
  async list(options: { state?: string; templateId?: string; cursor?: string; limit?: number } = {}) {
    const page = await this.client.listSandboxes(options)
    return { sandboxes: page.sandboxes.map((info) => new Sandbox(this.client, info)), nextCursor: page.nextCursor }
  }
}

export class VolumeResource {
  private readonly client: Client

  constructor(client: Client) {
    this.client = client
  }
  create(options: Record<string, unknown> = {}) { return this.client.createVolume(options) }
  list() { return this.client.listVolumes() }
}

export class TemplateResource {
  private readonly client: Client

  constructor(client: Client) {
    this.client = client
  }
  list() { return this.client.listTemplates() }
  get(templateId: string) { return this.client.getTemplate(templateId) }
  create(options: Record<string, unknown>) { return this.client.createTemplate(options) }
  exists(templateId: string) { return this.client.getTemplate(templateId).then(() => true).catch((error) => error instanceof APIError && error.status === 404 ? false : Promise.reject(error)) }
}

export class E2B {
  readonly client: Client
  readonly Sandbox: SandboxResource
  readonly Volume: VolumeResource
  readonly Template: TemplateResource

  constructor(options: ClientOptions = {}) {
    this.client = new Client(options)
    this.Sandbox = new SandboxResource(this.client)
    this.Volume = new VolumeResource(this.client)
    this.Template = new TemplateResource(this.client)
  }
}

export default Sandbox
