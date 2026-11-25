import assert from "node:assert/strict"
import net from "node:net"
import { Client, Sandbox } from "./index.ts"

class FrameReader {
  constructor(socket) {
    this.socket = socket
    this.iterator = socket[Symbol.asyncIterator]()
    this.buffer = Buffer.alloc(0)
  }

  async read(size) {
    while (this.buffer.length < size) {
      const next = await this.iterator.next()
      if (next.done) throw new Error("socket closed")
      this.buffer = Buffer.concat([this.buffer, next.value])
    }
    const value = this.buffer.subarray(0, size)
    this.buffer = this.buffer.subarray(size)
    return value
  }

  async until(marker) {
    for (;;) {
      const index = this.buffer.indexOf(marker)
      if (index >= 0) {
        const end = index + marker.length
        const value = this.buffer.subarray(0, end)
        this.buffer = this.buffer.subarray(end)
        return value
      }
      const next = await this.iterator.next()
      if (next.done) throw new Error("socket closed")
      this.buffer = Buffer.concat([this.buffer, next.value])
    }
  }

  async frame() {
    const header = await this.read(2)
    const opcode = header[0] & 0x0f
    let length = header[1] & 0x7f
    if (length === 126) length = this.read(2).then((value) => value.readUInt16BE(0))
    if (length === 127) length = Number((await this.read(8)).readBigUInt64BE(0))
    if (length instanceof Promise) length = await length
    const mask = header[1] & 0x80 ? await this.read(4) : null
    const payload = await this.read(length)
    if (mask) {
      for (let index = 0; index < payload.length; index += 1) payload[index] ^= mask[index % 4]
    }
    return { opcode, payload }
  }
}

function sendFrame(socket, payload, opcode = 1) {
  const data = Buffer.from(payload)
  const header = Buffer.from(data.length < 126 ? [0x80 | opcode, data.length] : [0x80 | opcode, 126, data.length >> 8, data.length & 255])
  socket.write(Buffer.concat([header, data]))
}

let reconnects = 0
const server = net.createServer(async (socket) => {
  const reader = new FrameReader(socket)
  try {
    const head = (await reader.until(Buffer.from("\r\n\r\n"))).toString().trim()
    const target = head.split("\r\n", 1)[0].split(" ", 3)[1]
    const reconnect = new URL(target, "ws://127.0.0.1").searchParams.get("pid") === "42"
    if (reconnect) reconnects += 1
    const headers = Object.fromEntries(head.split("\r\n").slice(1).map((line) => {
      const [key, ...value] = line.split(/:\s*/, 2)
      return [key.toLowerCase(), value.join(": ")]
    }))
    assert.match(headers["sec-websocket-protocol"], /kontinue-auth\./)
    assert.equal(headers["x-kontinue-sandbox-id"], undefined)
    assert.equal(headers["x-kontinue-token"], undefined)
    const key = headers["sec-websocket-key"]
    const { createHash } = await import("node:crypto")
    const accept = createHash("sha1").update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest("base64")
    socket.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\nSec-WebSocket-Protocol: kontinue\r\n\r\n`)
    sendFrame(socket, JSON.stringify({ type: "ready", pid: 42 }))
    if (reconnect) sendFrame(socket, "resumed")
    for (;;) {
      const frame = await reader.frame()
      if (frame.opcode === 8) {
        sendFrame(socket, "", 8)
        socket.end()
        return
      }
      const text = frame.payload.toString()
      if (text.startsWith('{"type":"resize"')) sendFrame(socket, "resized")
      else if (text === '{"type":"kill"}') {
        sendFrame(socket, JSON.stringify({ type: "exit", exitCode: 137 }))
        sendFrame(socket, "", 8)
        socket.end()
        return
      } else if (text === "finish") {
        sendFrame(socket, JSON.stringify({ type: "exit", exitCode: 7 }))
        sendFrame(socket, "", 8)
        socket.end()
        return
      } else sendFrame(socket, `echo:${text}`)
    }
  } catch (error) {
    socket.destroy(error)
  }
})

await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve))
try {
  const address = server.address()
  const endpoint = `http://127.0.0.1:${address.port}`
  const client = new Client({ baseUrl: endpoint, gatewayUrl: endpoint })
  const sandbox = new Sandbox(client, { sandboxId: "box", accessToken: "sandbox-token", state: "RUNNING" })
  const output = []
  const session = await sandbox.pty.create({ onData: (data) => output.push(typeof data === "string" ? data : new TextDecoder().decode(data)) })
  assert.equal(session.pid, 42)
  session.sendInput("hello\n")
  for (let attempt = 0; attempt < 20 && !output.includes("echo:hello\n"); attempt += 1) await new Promise((resolve) => setTimeout(resolve, 10))
  session.resize(120, 40)
  for (let attempt = 0; attempt < 20 && !output.includes("resized"); attempt += 1) await new Promise((resolve) => setTimeout(resolve, 10))
  session.close()
  await session.wait()
  assert.ok(output.includes("echo:hello\n"), output)
  assert.ok(output.includes("resized"), output)
  const exitCodes = []
  const reconnected = await sandbox.pty.connect(42, {
    onData: (data) => output.push(typeof data === "string" ? data : new TextDecoder().decode(data)),
    onExit: (code) => exitCodes.push(code),
  })
  reconnected.sendInput("finish")
  assert.equal(await reconnected.wait(), 7)
  assert.deepEqual(exitCodes, [7])
  assert.equal(reconnects, 1)
  console.log("typescript PTY WebSocket e2e passed")
} finally {
  server.close()
}
