package tools

import (
	"context"
	"strings"
	"testing"
)

func TestCodexRunnerRejectsModelSuppliedIdentity(t *testing.T) {
	tool := NewCodexRunnerTool("start")
	result := tool.Execute(context.Background(), map[string]any{
		"repo_id": "probe", "prompt": "test",
		"channel": "weixin", "chat_id": "forged",
		"session_key": "forged", "message_id": "forged",
	})
	if !strings.Contains(result.ForLLM, "可信入站") {
		t.Fatalf("expected trusted-context rejection, got %q", result.ForLLM)
	}
}
