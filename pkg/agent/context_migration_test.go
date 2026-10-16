//go:build !mipsle && !netbsd && !(freebsd && arm)

package agent

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sipeed/picoclaw/pkg/memory"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/seahorse"
	"github.com/sipeed/picoclaw/pkg/session"
)

func TestSeahorseMigrationKeepsLegacySummaryEventsAndRawHistory(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := memory.NewJSONLStore(filepath.Join(dir, "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	sessions := session.NewJSONLBackend(store)
	sessions.SetSummary("chat", "用户将提醒时间从16:40更正为18:40，取消旧任务")
	for i := range 100 {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		sessions.AddMessage("chat", role, fmt.Sprintf("原始消息%03d：%s", i, strings.Repeat("上下文内容", 30)))
	}
	if err := sessions.Save("chat"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "seahorse.db")
	complete := func(_ context.Context, prompt string, _ seahorse.CompleteOptions) (string, error) {
		return "保留用户更正：提醒18:40；旧任务已取消；其余内容可检索原文。", nil
	}
	engine, err := seahorse.NewEngine(seahorse.Config{DBPath: path}, complete)
	if err != nil {
		t.Fatal(err)
	}
	mgr := &seahorseContextManager{engine: engine, sessions: sessions}
	if err := mgr.ReconcileSession(ctx, "chat"); err != nil {
		t.Fatal(err)
	}
	// A delivered external result is appended after initialization, then retried.
	for range 2 {
		if err := sessions.AddMessageOnce("chat", "codex-result:job-1", providers.Message{Role: "assistant", Content: "任务job-1已完成，测试通过"}); err != nil {
			t.Fatal(err)
		}
		if err := mgr.ReconcileSession(ctx, "chat"); err != nil {
			t.Fatal(err)
		}
	}
	check := func(e *seahorse.Engine) {
		t.Helper()
		s := e.GetRetrieval().Store()
		conv, err := s.GetConversationBySessionKey(ctx, "chat")
		if err != nil || conv == nil {
			t.Fatal("missing conversation", err)
		}
		messages, err := s.GetMessages(ctx, conv.ConversationID, 0, 0)
		if err != nil || len(messages) != 102 {
			t.Fatalf("expected summary + 100 originals + 1 result; got %d, %v", len(messages), err)
		}
		if !strings.Contains(messages[0].Content, "18:40") || !strings.Contains(messages[101].Content, "job-1") {
			t.Fatal("lost checkpoint or background result")
		}
		result, err := e.GetRetrieval().Grep(ctx, seahorse.GrepInput{Pattern: "%原始消息000%", ConversationID: conv.ConversationID})
		if err != nil || len(result.Messages) != 1 {
			t.Fatalf("original message not retrievable: %v %v", result, err)
		}
	}
	check(engine)
	if _, err := engine.Compact(ctx, "chat", seahorse.CompactInput{Force: true}); err != nil {
		t.Fatal(err)
	}
	check(engine)
	if len(sessions.GetHistory("chat")) != 101 {
		t.Fatal("compaction deleted the transcript")
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	engine, err = seahorse.NewEngine(seahorse.Config{DBPath: path}, complete)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	mgr.engine = engine
	if err := mgr.ReconcileSession(ctx, "chat"); err != nil {
		t.Fatal(err)
	}
	check(engine)
	assembled, err := mgr.Assemble(ctx, &AssembleRequest{SessionKey: "chat", Budget: 100000, MaxTokens: 4096})
	if err != nil || assembled == nil {
		t.Fatal("cannot assemble after restart", err)
	}
	if !strings.Contains(assembled.Summary, "18:40") {
		t.Fatal("new summary lost user correction")
	}
}
