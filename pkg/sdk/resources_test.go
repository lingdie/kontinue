package sdk

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestE2BResourceSDKUsesSandboxGatewayCredentials(t *testing.T) {
	const sandboxID = "sbx-test"
	const accessToken = "sandbox-token"
	var wrote []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Kontinue-Sandbox-ID") != sandboxID {
			t.Fatalf("sandbox header = %q", r.Header.Get("X-Kontinue-Sandbox-ID"))
		}
		if r.Header.Get("X-Kontinue-Token") != accessToken {
			t.Fatalf("token header = %q", r.Header.Get("X-Kontinue-Token"))
		}
		switch r.URL.Path {
		case "/v1/process/exec":
			var request struct {
				Command []string `json:"command"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatal(err)
			}
			if len(request.Command) != 3 || (request.Command[2] != "printf hello" && request.Command[2] != "printf streamed") {
				t.Fatalf("command payload = %#v", request.Command)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"pid": 7, "stdout": "hello", "stderr": "", "exitCode": 0})
		case "/v1/process/logs/stream":
			_, _ = io.WriteString(w, `{"type":"stdout","data":"streamed"}`+"\n"+`{"type":"exit","exitCode":0}`+"\n")
		case "/v1/files/stream":
			var err error
			wrote, err = io.ReadAll(r.Body)
			if err != nil {
				t.Fatal(err)
			}
			w.WriteHeader(http.StatusNoContent)
		case "/v1/files":
			_, _ = w.Write([]byte("hello"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := NewClientWithOptions(ClientOptions{BaseURL: server.URL, GatewayURL: server.URL, HTTPClient: server.Client()})
	sandbox := client.attachSandbox(&Sandbox{SandboxID: sandboxID, AccessToken: accessToken})
	result, err := sandbox.Commands.Run(context.Background(), "printf hello", CommandRunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Stdout != "hello" {
		t.Fatalf("stdout = %q", result.Stdout)
	}
	var chunks []string
	streamed, err := sandbox.Commands.Run(context.Background(), "printf streamed", CommandRunOptions{OnStdout: func(chunk string) { chunks = append(chunks, chunk) }})
	if err != nil {
		t.Fatal(err)
	}
	if streamed.Stdout != "streamed" || len(chunks) != 1 || chunks[0] != "streamed" {
		t.Fatalf("streamed result = %#v callbacks = %#v", streamed, chunks)
	}
	if err := sandbox.Files.Write(context.Background(), "/tmp/hello", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if string(wrote) != "hello" {
		t.Fatalf("written bytes = %q", wrote)
	}
	content, err := sandbox.Files.Read(context.Background(), "/tmp/hello")
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "hello" {
		t.Fatalf("read bytes = %q", content)
	}
}
