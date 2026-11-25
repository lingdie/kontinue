import base64
import hashlib
import json
import socketserver
import struct
import threading
import unittest
import urllib.parse

from sdk.python.kontinue import Client, PtyCreateOptions, Sandbox


def read_exact(sock, size):
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise EOFError
        data += chunk
    return data


def read_frame(sock):
    first, second = read_exact(sock, 2)
    opcode = first & 0x0F
    length = second & 0x7F
    if length == 126:
        length = struct.unpack("!H", read_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", read_exact(sock, 8))[0]
    mask = read_exact(sock, 4) if second & 0x80 else b""
    payload = bytearray(read_exact(sock, length))
    if mask:
        for index in range(length):
            payload[index] ^= mask[index % 4]
    return opcode, bytes(payload)


def write_frame(sock, payload, opcode=1):
    length = len(payload)
    header = bytearray([0x80 | opcode])
    if length < 126:
        header.append(length)
    elif length < 65536:
        header.append(126)
        header.extend(struct.pack("!H", length))
    else:
        header.append(127)
        header.extend(struct.pack("!Q", length))
    sock.sendall(bytes(header) + payload)


class PtyServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True


class PtyHandler(socketserver.BaseRequestHandler):
    def handle(self):
        request = b""
        while b"\r\n\r\n" not in request:
            request += self.request.recv(4096)
        head = request.split(b"\r\n\r\n", 1)[0].decode()
        target = head.split("\r\n", 1)[0].split(" ", 2)[1]
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(target).query)
        reconnect = query.get("pid") == ["42"]
        self.server.reconnect_seen = reconnect
        headers = dict(line.split(": ", 1) for line in head.split("\r\n")[1:])
        self.server.credentials = (headers.get("X-Kontinue-Sandbox-ID"), headers.get("X-Kontinue-Token"))
        key = headers["Sec-WebSocket-Key"]
        accept = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
        self.request.sendall((
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Accept: {accept}\r\n"
            "Sec-WebSocket-Protocol: kontinue\r\n\r\n"
        ).encode())
        write_frame(self.request, b'{"type":"ready","pid":42}')
        if reconnect:
            write_frame(self.request, b"resumed")
        while True:
            opcode, payload = read_frame(self.request)
            if opcode == 8:
                return
            if payload.startswith(b'{'):
                try:
                    message = json.loads(payload)
                except json.JSONDecodeError:
                    message = {}
                if message.get("type") == "resize":
                    write_frame(self.request, b"resized")
                    continue
                if message.get("type") == "kill":
                    write_frame(self.request, b'{"type":"exit","exitCode":137}')
                    write_frame(self.request, b"", opcode=8)
                    return
            if payload == b"finish":
                write_frame(self.request, b'{"type":"exit","exitCode":7}')
                write_frame(self.request, b"", opcode=8)
                return
            write_frame(self.request, b"echo:" + payload)


class PtyE2ETest(unittest.TestCase):
    def test_interactive_pty_websocket(self):
        server = PtyServer(("127.0.0.1", 0), PtyHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        output = []
        events = threading.Event()
        try:
            endpoint = f"ws://127.0.0.1:{server.server_address[1]}"
            client = Client(base_url=endpoint, gateway_url=endpoint)
            sandbox = Sandbox(client, {"sandboxId": "box", "accessToken": "sandbox-token"})

            def on_data(data):
                output.append(data.decode() if isinstance(data, bytes) else data)
                if "resized" in output:
                    events.set()

            session = sandbox.pty.create(PtyCreateOptions(on_data=on_data))
            self.assertEqual(session.pid, 42)
            session.send_input("hello\n")
            session.resize(120, 40)
            self.assertTrue(events.wait(2))
            session.close()
            self.assertTrue(session._done.wait(2))
            self.assertIn("echo:hello\n", output)
            self.assertIn("resized", output)
            self.assertEqual(server.credentials, ("box", "sandbox-token"))
            exit_codes = []
            reconnected = sandbox.pty.connect(
                42,
                PtyCreateOptions(on_data=on_data, on_exit=exit_codes.append),
            )
            reconnected.send_input("finish")
            self.assertEqual(reconnected.wait(2), 7)
            self.assertEqual(exit_codes, [7])
            self.assertTrue(server.reconnect_seen)
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
