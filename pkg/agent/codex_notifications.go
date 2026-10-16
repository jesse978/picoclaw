package agent

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/tools"
)

func (al *AgentLoop) codexNotificationsEnabled() bool {
	return os.Getenv("PICOCLAW_CODEX_RUNNER_SOCKET") != ""
}

func (al *AgentLoop) codexNotificationLoop(ctx context.Context) {
	if _, err := tools.CodexRunnerRequest(ctx, map[string]any{"op": "notification_reconcile"}); err != nil {
		logger.WarnCF("codex-runner", "Notification recovery failed", map[string]any{"error": err.Error()})
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			al.sendOneCodexNotification(ctx)
		}
	}
}

func (al *AgentLoop) sendOneCodexNotification(ctx context.Context) {
	response, err := tools.CodexRunnerRequest(ctx, map[string]any{"op": "notifications"})
	if err != nil {
		logger.WarnCF("codex-runner", "Notification poll failed", map[string]any{"error": err.Error()})
		return
	}
	if response["data"] == nil {
		return
	}
	item, ok := response["data"].(map[string]any)
	if !ok {
		return
	}
	id, _ := item["id"].(string)
	channel, _ := item["channel"].(string)
	chatID, _ := item["chat_id"].(string)
	sessionKey, _ := item["session_key"].(string)
	status, _ := item["status"].(string)
	result, _ := item["result"].(string)
	verification, _ := item["verification"].(string)
	if text := []rune(result); len(text) > 3000 {
		result = string(text[:3000]) + "\n…详情请查询任务结果"
	}
	title := "Codex 任务 " + id + "：" + status
	if status == "waiting_answer" {
		title += "\n需要你的回答。回复时请引用这个任务 ID。"
	}
	content := strings.TrimSpace(title + "\n验证：" + verification + "\n" + result)
	sendErr := fmt.Errorf("渠道不可用")
	if al.channelManager != nil {
		if ch, exists := al.channelManager.GetChannel(channel); exists {
			sendCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
			_, sendErr = ch.Send(sendCtx, bus.OutboundMessage{
				Channel: channel, ChatID: chatID,
				Context:    bus.NewOutboundContext(channel, chatID, ""),
				SessionKey: sessionKey, Content: content,
			})
			cancel()
		}
	}
	ack := map[string]any{"op": "notification_ack", "job_id": id, "sent": sendErr == nil}
	if sendErr != nil {
		ack["error"] = sendErr.Error()
	}
	if _, err := tools.CodexRunnerRequest(ctx, ack); err != nil {
		logger.WarnCF("codex-runner", "Notification acknowledgement failed", map[string]any{"job_id": id, "error": err.Error()})
	}
}

// Import acknowledged results at the next inbound turn, before assembling its
// context. The runner is the durable inbox; querying it never sends a message.
// Importing here avoids inserting an assistant message inside a live tool chain.
func (al *AgentLoop) syncCodexNotificationHistory(ctx context.Context, agent *AgentInstance,
	msg bus.InboundMessage, sessionKey string,
) {
	if !al.codexNotificationsEnabled() || msg.Context.MessageID == "" || strings.HasPrefix(sessionKey, "agent:cron-") {
		return
	}
	store, ok := agent.Sessions.(interface {
		AddMessageOnce(string, string, providers.Message) error
	})
	if !ok {
		return
	}
	response, err := tools.CodexRunnerRequest(ctx, map[string]any{
		"op": "conversation_results", "channel": msg.Channel, "chat_id": msg.ChatID,
		"session_key": sessionKey, "message_id": msg.Context.MessageID,
	})
	if err != nil {
		logger.WarnCF("codex-runner", "Task context import failed", map[string]any{"error": err.Error()})
		return
	}
	items, _ := response["data"].([]any)
	for _, raw := range items {
		item, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		id, _ := item["id"].(string)
		if id == "" {
			continue
		}
		result, _ := item["final_message"].(string)
		if result == "" {
			result, _ = item["error"].(string)
		}
		if text := []rune(result); len(text) > 8000 {
			result = string(text[:8000]) + "\n[Full result available via coding_status.]"
		}
		prompt, _ := item["prompt"].(string)
		if text := []rune(prompt); len(text) > 1000 {
			prompt = string(text[:1000])
		}
		content := fmt.Sprintf("[Previously delivered Codex task result at %v]\nTask ID: %s\nOriginal request: %s\nStatus: %v; verification: %v\n%s",
			item["notification_at"], id, prompt, item["status"], item["verification"], result)
		if err := store.AddMessageOnce(sessionKey, "codex-result:"+id, providers.Message{Role: "assistant", Content: content}); err != nil {
			logger.WarnCF("codex-runner", "Task context persistence failed", map[string]any{"job_id": id, "error": err.Error()})
			return
		}
	}
}
