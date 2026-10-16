package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"time"
)

// CodexRunnerRequest talks to a local runner. The runner socket is configured
// by the service operator; the model cannot choose it.
func CodexRunnerRequest(ctx context.Context, payload map[string]any) (map[string]any, error) {
	path := os.Getenv("PICOCLAW_CODEX_RUNNER_SOCKET")
	if path == "" {
		return nil, errors.New("编码 runner 未配置")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if err := json.NewEncoder(conn).Encode(payload); err != nil {
		return nil, err
	}
	var answer map[string]any
	if err := json.NewDecoder(conn).Decode(&answer); err != nil {
		return nil, err
	}
	if answer["ok"] != true {
		return nil, fmt.Errorf("%v", answer["error"])
	}
	return answer, nil
}

type CodexRunnerTool struct{ operation string }

func NewCodexRunnerTool(operation string) *CodexRunnerTool {
	return &CodexRunnerTool{operation: operation}
}
func (t *CodexRunnerTool) Name() string { return "coding_" + t.operation }
func (t *CodexRunnerTool) Description() string {
	switch t.operation {
	case "start":
		return "Submit a repository coding task to Codex; returns a durable job ID immediately."
	case "status":
		return "Read actual status and result of a Codex coding job in this conversation."
	case "cancel":
		return "Cancel a Codex coding job in this conversation."
	case "answer":
		return "Answer a question from a waiting Codex job using its exact parent job ID."
	}
	return ""
}
func (t *CodexRunnerTool) Parameters() map[string]any {
	props := map[string]any{}
	required := []string{}
	switch t.operation {
	case "start", "answer":
		props["repo_id"] = map[string]any{"type": "string", "description": "Repository ID configured by the operator and listed in the workspace instructions. Never invent another ID or substitute another repository after an error."}
		props["prompt"] = map[string]any{"type": "string", "description": "Full user request or answer."}
		props["read_only"] = map[string]any{"type": "boolean", "description": "Use a read-only Codex sandbox for analysis."}
		required = append(required, "repo_id", "prompt")
		if t.operation == "answer" {
			props["parent_job_id"] = map[string]any{"type": "string"}
			required = append(required, "parent_job_id")
		} else {
			props["parent_job_id"] = map[string]any{"type": "string", "description": "Exact job ID for an explicit follow-up."}
		}
	case "status", "cancel":
		props["job_id"] = map[string]any{"type": "string"}
		required = append(required, "job_id")
	}
	return map[string]any{"type": "object", "properties": props, "required": required,
		"additionalProperties": false}
}
func (t *CodexRunnerTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	channel, chatID := ToolChannel(ctx), ToolChatID(ctx)
	sessionKey, messageID := ToolSessionKey(ctx), ToolMessageID(ctx)
	if channel == "" || chatID == "" || sessionKey == "" || messageID == "" {
		return ErrorResult("缺少可信入站会话或消息 ID，不能启动编码任务")
	}
	req := map[string]any{"op": t.operation, "channel": channel, "chat_id": chatID,
		"session_key": sessionKey, "message_id": messageID}
	for _, key := range []string{"repo_id", "prompt", "parent_job_id", "job_id", "read_only"} {
		if val, ok := args[key]; ok {
			req[key] = val
		}
	}
	answer, err := CodexRunnerRequest(ctx, req)
	if err != nil {
		return ErrorResult(err.Error())
	}
	data, err := json.Marshal(answer["data"])
	if err != nil {
		return ErrorResult(err.Error())
	}
	return NewToolResult(string(data))
}
