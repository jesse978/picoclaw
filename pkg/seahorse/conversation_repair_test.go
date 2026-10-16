package seahorse

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/sipeed/picoclaw/pkg/tools"
)

func TestBootstrapRemovesRolledBackTail(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	conv, msgs := prepareBootstrapRepairConversation(t, e, ctx, "chat")
	if _, err := e.Ingest(ctx, "chat", []Message{{Role: "user", Content: "cancelled turn", TokenCount: 5}}); err != nil {
		t.Fatal(err)
	}
	if err := e.Bootstrap(ctx, "chat", msgs); err != nil {
		t.Fatal(err)
	}
	got, err := e.store.GetMessages(ctx, conv.ConversationID, 0, 0)
	if err != nil || len(got) != len(msgs) {
		t.Fatalf("stale tail survived rollback: %+v %v", got, err)
	}
}

func TestRetrievalToolsStayInCurrentConversation(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	a, _ := s.GetOrCreateConversation(ctx, "chat-a")
	b, _ := s.GetOrCreateConversation(ctx, "chat-b")
	one, _ := s.AddMessage(ctx, a.ConversationID, "user", "sharedkeyword own-note", 5)
	two, _ := s.AddMessage(ctx, b.ConversationID, "user", "sharedkeyword other-private-note", 5)
	r := &RetrievalEngine{store: s}
	ctx = tools.WithToolSessionContext(ctx, "main", "chat-a", nil)
	result := NewGrepTool(r).Execute(ctx, map[string]any{"pattern": "%sharedkeyword%", "all_conversations": true})
	if result.IsError || !strings.Contains(result.ForLLM, "own-note") || strings.Contains(result.ForLLM, "other-private-note") {
		t.Fatalf("search crossed sessions: %+v", result)
	}
	expand := NewExpandTool(r)
	result = expand.Execute(ctx, map[string]any{"message_ids": []any{fmt.Sprint(two.ID)}})
	if !result.IsError || strings.Contains(result.ForLLM, "other-private-note") {
		t.Fatal("expansion crossed sessions")
	}
	result = expand.Execute(ctx, map[string]any{"message_ids": []any{fmt.Sprint(one.ID)}})
	if result.IsError || !strings.Contains(result.ForLLM, "own-note") {
		t.Fatal("own message cannot be retrieved")
	}
}

func TestSummaryRetainsRolesAndEvidenceWithoutInternalToolFormat(t *testing.T) {
	msgs := []Message{
		{Role: "user", Content: "把提醒改为18:40，取消旧计划"},
		{Role: "assistant", Content: "[tool_use: cron]", Parts: []MessagePart{{Type: "tool_use", Name: "cron", Arguments: `{"action":"update","job_id":"parcel-1"}`}}},
		{Role: "tool", Parts: []MessagePart{{Type: "tool_result", Text: "update failed: job not found"}}},
	}
	text := formatMessagesForSummary(msgs)
	for _, want := range []string{"role=user", "18:40", "parcel-1", "update failed"} {
		if !strings.Contains(text, want) {
			t.Fatalf("lost %q: %s", want, text)
		}
	}
	if strings.Contains(text, "[tool_use:") {
		t.Fatal("internal tool syntax leaked into summary prompt")
	}
	text = truncateSummary([]Message{{Role: "user", Content: strings.Repeat("中", 3000)}})
	if !utf8.ValidString(text) {
		t.Fatal("fallback summary splits a Chinese character")
	}
}
