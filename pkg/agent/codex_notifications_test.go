package agent

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/media"
	"github.com/sipeed/picoclaw/pkg/memory"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/session"
)

func TestCodexDeliveredResultsImportedOnceWithTrustedIdentity(t *testing.T) {
	dir, err := os.MkdirTemp("", "pico-notify-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "runner.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	t.Setenv("PICOCLAW_CODEX_RUNNER_SOCKET", path)
	requests := make(chan map[string]any, 2)
	go func() {
		for range 2 {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			var req map[string]any
			if json.NewDecoder(conn).Decode(&req) == nil {
				requests <- req
				_ = json.NewEncoder(conn).Encode(map[string]any{"ok": true, "data": []any{
					map[string]any{"id": "job-1", "status": "succeeded", "prompt": "修复提醒", "verification": "tests passed", "final_message": "已完成，提醒改为18:40", "notification_at": "2026-10-16T00:00:00Z"},
				}})
			}
			_ = conn.Close()
		}
	}()
	s, err := memory.NewJSONLStore(filepath.Join(dir, "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	agent := &AgentInstance{Sessions: session.NewJSONLBackend(s)}
	al := &AgentLoop{}
	msg := bus.NormalizeInboundMessage(bus.InboundMessage{Context: bus.InboundContext{Channel: "weixin", ChatID: "owner", MessageID: "incoming-1"}})
	for range 2 {
		al.syncCodexNotificationHistory(context.Background(), agent, msg, "chat")
	}
	history := agent.Sessions.GetHistory("chat")
	if len(history) != 1 || !strings.Contains(history[0].Content, "18:40") || !strings.Contains(history[0].Content, "tests passed") {
		t.Fatalf("missing/duplicate result: %+v", history)
	}
	for range 2 {
		req := <-requests
		if req["op"] != "conversation_results" || req["session_key"] != "chat" || req["message_id"] != "incoming-1" || req["chat_id"] != "owner" {
			t.Fatalf("wrong trusted identity: %v", req)
		}
	}
}

func TestMissingAttachmentIsExplicitInModelContext(t *testing.T) {
	input := []providers.Message{{Role: "user", Content: "上面那张图呢", Media: []string{"media://missing"}}}
	result := resolveMediaRefs(input, media.NewFileMediaStore(), 1024, 0)
	if !strings.Contains(result[0].Content, "Attachment unavailable") || len(result[0].Media) != 0 {
		t.Fatalf("missing attachment silently disappeared: %+v", result)
	}
	if input[0].Content != "上面那张图呢" {
		t.Fatal("input transcript mutated")
	}
}
