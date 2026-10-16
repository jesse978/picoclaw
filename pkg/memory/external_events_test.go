package memory

import (
	"context"
	"sync"
	"testing"

	"github.com/sipeed/picoclaw/pkg/providers"
)

func TestExternalEventDedupAcrossRetryCompactionAndRestart(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	msg := providers.Message{Role: "assistant", Content: "Task finished; tests passed"}
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.AddMessageOnce(ctx, "chat", "job:1", msg); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	history, err := s.GetHistory(ctx, "chat")
	if err != nil || len(history) != 1 {
		t.Fatalf("duplicate delivery: history=%v, err=%v", history, err)
	}
	// Simulate a crash after appending the event but before committing its ID.
	meta, _ := s.readMeta("chat")
	meta.ExternalEvents = nil
	if err := s.writeMeta("chat", meta); err != nil {
		t.Fatal(err)
	}
	if err := s.AddMessageOnce(ctx, "chat", "job:1", msg); err != nil {
		t.Fatal(err)
	}
	history, _ = s.GetHistory(ctx, "chat")
	if len(history) != 1 {
		t.Fatal("append recovery duplicated event")
	}
	if err := s.AddMessage(ctx, "chat", "user", "continue"); err != nil {
		t.Fatal(err)
	}
	if err := s.TruncateHistory(ctx, "chat", 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(ctx, "chat"); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewJSONLStore(s.dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.AddMessageOnce(ctx, "chat", "job:1", msg); err != nil {
		t.Fatal(err)
	}
	history, _ = reopened.GetHistory(ctx, "chat")
	if len(history) != 1 || history[0].Content != "continue" {
		t.Fatalf("compacted event reappeared: %v", history)
	}
	if err := reopened.AddMessageOnce(ctx, "other-chat", "job:1", msg); err != nil {
		t.Fatal(err)
	}
	history, _ = reopened.GetHistory(ctx, "other-chat")
	if len(history) != 1 {
		t.Fatal("dedup leaked across sessions")
	}
}
