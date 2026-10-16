package tools

import (
	"context"
	"strings"
	"testing"

	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/cron"
)

func TestScheduledExecutionCannotCreateOrChangeSchedules(t *testing.T) {
	tool := newTestCronToolWithExecutorAndConfig(t, &stubJobExecutor{}, config.DefaultConfig())
	ctx := WithToolSessionContext(context.Background(), "main", "agent:cron-job-1-run", nil)
	for _, action := range []string{"add", "update", "remove", "enable", "disable"} {
		result := tool.Execute(ctx, map[string]any{"action": action})
		if !result.IsError || !strings.Contains(result.ForLLM, "Scheduling mutations are disabled") {
			t.Fatalf("%s was not blocked: %+v", action, result)
		}
	}
}

func TestScheduledNoReplyDoesNotPublish(t *testing.T) {
	executor := &stubJobExecutor{response: "  NO_REPLY\n"}
	tool := newTestCronToolWithExecutorAndConfig(t, executor, config.DefaultConfig())
	job := &cron.CronJob{ID: "quiet"}
	job.Payload.Channel = "weixin"
	job.Payload.To = "test-chat"
	job.Payload.Message = "检查是否有新进展，没有则不通知"
	if got := tool.ExecuteJob(context.Background(), job); got != "ok" {
		t.Fatal(got)
	}
	if executor.publishedResp != "" {
		t.Fatal("silent result was sent")
	}
}
