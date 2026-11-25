package sdk

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/websocket"
)

func TestPtyWebSocketEndToEnd(t *testing.T) {
	var mu sync.Mutex
	var received []string
	handler := websocket.Handler(func(conn *websocket.Conn) {
		if conn.Request().Header.Get("X-Kontinue-Sandbox-ID") != "box" || conn.Request().Header.Get("X-Kontinue-Token") != "sandbox-token" {
			t.Errorf("missing sandbox credentials")
			return
		}
		if err := websocket.Message.Send(conn, []byte(`{"type":"ready","pid":77}`)); err != nil {
			return
		}
		reconnect := conn.Request().URL.Query().Get("pid") == "77"
		if reconnect {
			_ = websocket.Message.Send(conn, []byte("resumed"))
		}
		for {
			var payload []byte
			if err := websocket.Message.Receive(conn, &payload); err != nil {
				return
			}
			var control struct {
				Type string `json:"type"`
				Cols int    `json:"cols"`
				Rows int    `json:"rows"`
			}
			if json.Unmarshal(payload, &control) == nil && control.Type == "resize" {
				_ = websocket.Message.Send(conn, []byte("resized"))
				continue
			}
			if json.Unmarshal(payload, &control) == nil && control.Type == "kill" {
				_ = websocket.Message.Send(conn, []byte(`{"type":"exit","exitCode":137}`))
				_ = conn.Close()
				return
			}
			if string(payload) == "finish" {
				_ = websocket.Message.Send(conn, []byte(`{"type":"exit","exitCode":7}`))
				_ = conn.Close()
				return
			}
			mu.Lock()
			received = append(received, string(payload))
			mu.Unlock()
			_ = websocket.Message.Send(conn, append([]byte("echo:"), payload...))
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	client := NewClientWithOptions(ClientOptions{BaseURL: server.URL, GatewayURL: server.URL, HTTPClient: server.Client()})
	sandbox := client.attachSandbox(&Sandbox{SandboxID: "box", AccessToken: "sandbox-token"})
	var outputMu sync.Mutex
	var output []string
	outputEvents := make(chan string, 4)
	session, err := sandbox.PTY.Create(context.Background(), PtyCreateOptions{
		Cmd:  "/bin/sh",
		Cols: 80,
		Rows: 24,
		OnData: func(data []byte) {
			outputMu.Lock()
			output = append(output, string(data))
			outputMu.Unlock()
			outputEvents <- string(data)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if session.PID != 77 {
		t.Fatalf("PTY PID = %d", session.PID)
	}
	if err := session.SendInput([]byte("hello\n")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-outputEvents:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for PTY echo")
	}
	if err := session.Resize(120, 40); err != nil {
		t.Fatal(err)
	}
	select {
	case <-outputEvents:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for PTY resize response")
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}

	outputMu.Lock()
	joined := strings.Join(output, "|")
	outputMu.Unlock()
	if !strings.Contains(joined, "echo:hello\n") || !strings.Contains(joined, "resized") {
		t.Fatalf("PTY output = %q", joined)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 || received[0] != "hello\n" {
		t.Fatalf("PTY input = %#v", received)
	}
	exitCodes := make(chan int, 1)
	reconnected, err := sandbox.PTY.Connect(77, PtyCreateOptions{OnExit: func(code int) { exitCodes <- code }})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconnected.SendInput([]byte("finish")); err != nil {
		t.Fatal(err)
	}
	if err := reconnected.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case code := <-exitCodes:
		if code != 7 {
			t.Fatalf("PTY exit code = %d", code)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for PTY exit event")
	}
}
