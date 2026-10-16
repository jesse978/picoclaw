# Local Codex runner

Optional single-worker queue used by the native `coding_start`, `coding_status`,
`coding_cancel`, and `coding_answer` tools. It requires Python 3 and an authenticated
Codex CLI. Run it as the same operating-system user as the gateway.

Copy `runner.py` to a private runtime directory and create `config.json` alongside
it. The JSON contains `codex_bin`, `model`, `timeout_seconds`, `channels`,
`allow_chat_ids`, `repo_roots`, and a `repos` mapping from stable IDs to absolute
repository paths. Use local values; do not commit that configuration.

Set `PICOCLAW_CODEX_RUNNER_SOCKET` in the gateway to `<runtime-directory>/runner.sock`.
See `python3 runner.py --help` for the service entry point. The socket is a trusted
local transport; it is not designed to be exposed over a network. Normal coding
jobs retain the existing full-access configuration; explicitly read-only requests
use a read-only sandbox. Configure the account and repository allowlist accordingly.

Job identity comes from the gateway's inbound context, not model arguments.
Completion and verification are separate fields. Notifications are acknowledged
only after channel delivery; ambiguous delivery stays `uncertain`. A read-only
`conversation_results` request returns recent acknowledged results from the same
owner and session, so the next turn can refer to the task result.

Tests use temporary repositories and a fake CLI:

```sh
python3 -m unittest test_runner test_conversation_repair
```

Keep `state.sqlite3`, its online backup, and the Codex session files when moving
or restoring the runner. No conversation history, runtime logs or credentials
belong in this source directory.
