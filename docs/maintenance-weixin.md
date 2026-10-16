# 微信机器人维护分支

维护仓库：<https://github.com/jesse978/picoclaw>

部署分支：`maintenance/weixin-reliability`，基于官方 `v0.3.1`
（`2cf030d2fd3b871d7ec17e3be34c24688aac76da`）。`upstream` 保留为
<https://github.com/sipeed/picoclaw>；`origin` 指向本 fork。

## 本分支的行为

- 启用官方 Seahorse 时，导入旧摘要，并在新一轮对话前同步 JSONL 原文。
  压缩保留 SQLite 中的原始消息；`short_grep`、`short_expand` 限制在当前会话。
  已经被旧版删除的原文无法从摘要还原。
- 已确认发出的后台 Codex 任务结果在下一条用户消息前进入上下文，按事件 ID
  去重；不把后台结果插入正在执行的工具调用序列。
- 图片引用索引落盘，服务重启后可恢复。图片文件目录和保留期由部署配置决定。
  文件失效时明确提示模型，避免猜测附件内容。
- 定时任务执行时禁止用 cron 工具创建或修改定时任务；`NO_REPLY` 不触发最终通知。
- Shell URL／路径规则测试直接调用 `guardCommand`，不启动 shell、不实际下载。
  运行时的危险命令和越界路径拦截规则保持原样。
- 保留本机原有的原生 Codex runner 工具。runner 源码和测试在
  `extras/codex-runner/`，运行数据、凭据和个人对话不进入 Git。

## 配置

合并以下字段到现有配置，不要覆盖渠道凭据或其他设置：

```json
{
  "agents": {"defaults": {"context_manager": "seahorse"}},
  "tools": {"media_cleanup": {
    "enabled": true,
    "max_age_minutes": 10080,
    "interval_minutes": 5
  }}
}
```

服务环境中将 `PICOCLAW_MEDIA_DIR` 设为持久化的绝对目录，避免系统清理临时文件。
媒体索引位于 `<workspace>/state/media-refs`，对话数据库位于
`<workspace>/sessions/seahorse.db`。备份时同时保留 JSONL、元数据和 SQLite；
运行中的 SQLite 使用在线备份 API，不只复制 `.db` 文件。

## 校验与构建

```sh
go test -p 1 ./pkg/memory ./pkg/session ./pkg/seahorse ./pkg/agent ./pkg/tools ./pkg/media ./pkg/gateway
cd extras/codex-runner
python3 -m unittest test_runner test_conversation_repair
cd ../..
go build -p 1 -trimpath -ldflags '-s -w' -o build/picoclaw ./cmd/picoclaw
```

在内存较小的机器上使用 `GOMAXPROCS=2 GOMEMLIMIT=512MiB` 降低构建峰值。
测试使用隔离目录和模拟 provider，不向真实微信会话发送测试消息。

## 后续同步

1. `git fetch upstream --tags`，核对正式发布说明及相关源码差异。
2. 在单独分支合入新版或挑选相关修复，保留 runner 接口和会话格式兼容。
3. 执行上述测试，确认旧会话摘要、原文检索、任务结果去重和图片恢复。
4. 备份当前二进制、配置、runner 和会话数据，再替换已验证的构建并检查健康接口。
5. 记录部署提交与备份路径。回滚时恢复二进制及配套配置；对话数据先另行备份，
   避免覆盖升级后新增消息。不要让旧式摘要在唯一一份新对话记录上进行破坏性压缩。

被用户停用的汇报定时任务保持停用。渠道发送成功只代表接口确认，不代表用户已阅读。
