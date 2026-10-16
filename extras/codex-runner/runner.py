#!/usr/bin/env python3
"""Persistent, single-worker Codex queue.  The Unix socket is a local transport,
not an identity boundary: only the PicoClaw native tool may submit channel jobs.
"""
import argparse
import contextlib
import datetime as dt
import fcntl
import hashlib
import json
import os
import pathlib
import re
import selectors
import signal
import socket
import socketserver
import sqlite3
import subprocess
import sys
import threading
import time
import uuid

ROOT = pathlib.Path(__file__).resolve().parent
CONFIG = ROOT / "config.json"
DB = ROOT / "state.sqlite3"
SOCKET = ROOT / "runner.sock"
LOGS = ROOT / "logs"
TERMINAL = {"succeeded", "failed", "cancelled", "timed_out", "interrupted", "waiting_answer"}
FINISHED = {"succeeded", "failed", "cancelled", "timed_out", "interrupted"}
MAX_PROMPT = 50000
MAX_LOG = 10 * 1024 * 1024
ACTIVE = {"pgid": None}
ACTIVE_LOCK = threading.Lock()
WORKER_THREAD = None


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def config():
    data = json.loads(CONFIG.read_text())
    if not isinstance(data.get("repos"), dict):
        raise ValueError("config requires repos")
    return data


@contextlib.contextmanager
def db():
    c = sqlite3.connect(DB, timeout=30, isolation_level=None)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    try:
        with c:
            yield c
    finally:
        c.close()


def init():
    ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
    LOGS.mkdir(mode=0o700, exist_ok=True)
    os.chmod(ROOT, 0o700)
    os.chmod(LOGS, 0o700)
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS jobs (
          id TEXT PRIMARY KEY, owner TEXT NOT NULL, channel TEXT NOT NULL,
          chat_id TEXT NOT NULL, session_key TEXT NOT NULL, message_id TEXT NOT NULL,
          repo_id TEXT NOT NULL, prompt TEXT NOT NULL, parent_id TEXT,
          thread_id TEXT, status TEXT NOT NULL, created_at TEXT NOT NULL,
          started_at TEXT, ended_at TEXT, exit_code INTEGER, error TEXT,
          log_dir TEXT NOT NULL, model TEXT NOT NULL, sandbox TEXT NOT NULL,
          approval TEXT NOT NULL, git_head TEXT, git_before TEXT, git_after TEXT,
          git_before_hash TEXT, git_after_hash TEXT,
          process_group INTEGER, process_start TEXT, progress TEXT,
          final_message TEXT, verification TEXT NOT NULL DEFAULT 'unknown',
          requirement TEXT NOT NULL DEFAULT 'unknown',
          checks_json TEXT NOT NULL DEFAULT '[]',
          notification TEXT NOT NULL DEFAULT 'none',
          notification_at TEXT, notification_error TEXT,
          notification_attempts INTEGER NOT NULL DEFAULT 0,
          UNIQUE(channel, chat_id, message_id)
        );
        CREATE INDEX IF NOT EXISTS jobs_queue ON jobs(status, created_at);
        """)
        cols = {r[1] for r in c.execute("PRAGMA table_info(jobs)")}
        for name in ("git_before_hash", "git_after_hash"):
            if name not in cols:
                c.execute(f"ALTER TABLE jobs ADD COLUMN {name} TEXT")
    os.chmod(DB, 0o600)


def repo_path(repo_id):
    cfg = config()
    raw = cfg["repos"].get(repo_id)
    if not isinstance(raw, str) or not raw.startswith("/"):
        raise ValueError("未知 repo_id；允许值：" + ", ".join(sorted(cfg["repos"])))
    path = pathlib.Path(raw).resolve(strict=True)
    if not path.is_dir() or path == pathlib.Path("/"):
        raise ValueError("项目目录无效")
    allowed = [pathlib.Path(p).resolve(strict=True) for p in cfg.get("repo_roots", [])]
    if allowed and not any(path == root or root in path.parents for root in allowed):
        raise ValueError("项目路径超出允许范围")
    return str(path)


def git(repo, *args):
    p = subprocess.run(["git", "-C", repo, *args], capture_output=True,
                       text=True, timeout=10, check=False)
    if p.returncode:
        raise ValueError("Git 检查失败: " + p.stderr.strip()[:400])
    return p.stdout.strip()


def git_state(repo):
    root = git(repo, "rev-parse", "--show-toplevel")
    if pathlib.Path(root).resolve() != pathlib.Path(repo).resolve():
        raise ValueError("repo_id 必须指向 Git 根目录")
    return git(repo, "rev-parse", "HEAD"), git(repo, "status", "--porcelain=v1", "-uall")


def git_fingerprint(repo):
    """Hash tracked changes and the contents of untracked, nonignored files."""
    h = hashlib.sha256()
    diff = subprocess.run(["git", "-C", repo, "diff", "--binary", "HEAD", "--"],
                          capture_output=True, timeout=30, check=True).stdout
    h.update(diff)
    paths = subprocess.run(["git", "-C", repo, "ls-files", "--others",
                            "--exclude-standard", "-z"], capture_output=True,
                           timeout=30, check=True).stdout.split(b"\0")
    for raw in sorted(p for p in paths if p):
        h.update(b"\0path\0" + raw + b"\0")
        path = pathlib.Path(os.fsdecode(raw))
        target = pathlib.Path(repo) / path
        if target.is_symlink():
            h.update(b"symlink\0" + os.fsencode(os.readlink(target)))
        elif target.is_file():
            with target.open("rb") as f:
                for chunk in iter(lambda: f.read(1024 * 1024), b""):
                    h.update(chunk)
        else:
            h.update(b"missing")
    return h.hexdigest()


def visible(row):
    keys = ("id", "repo_id", "parent_id", "thread_id", "status", "created_at",
            "started_at", "ended_at", "exit_code", "error", "progress",
            "final_message", "verification", "requirement", "checks_json", "notification",
            "notification_error", "git_head", "git_after")
    return {k: row[k] for k in keys}


def require_identity(req):
    for key in ("channel", "chat_id", "session_key", "message_id"):
        if not isinstance(req.get(key), str) or not req[key] or len(req[key]) > 500:
            raise ValueError("缺少可信入站身份或消息 ID: " + key)
    if req["channel"] not in config().get("channels", ["weixin"]):
        raise ValueError("渠道未授权")
    allowed = config().get("allow_chat_ids", [])
    if not allowed or req["chat_id"] not in allowed:
        raise ValueError("当前聊天未授权使用编码入口")
    return req["channel"] + ":" + req["chat_id"]


def own(c, req, job_id):
    owner = require_identity(req)
    row = c.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    if not row:
        raise ValueError("任务不存在")
    if row["owner"] != owner or row["session_key"] != req["session_key"]:
        raise ValueError("任务不属于当前会话")
    return row


def handle(req):
    op = req.get("op")
    if op == "health":
        return {"ok": WORKER_THREAD is not None and WORKER_THREAD.is_alive()}
    if op == "notification_reconcile":
        with db() as c:
            changed = c.execute("""UPDATE jobs SET notification='uncertain',
                            notification_error='Gateway 在发送确认前重启；未自动重发'
                            WHERE notification='sending'""").rowcount
            return {"uncertain": changed}
    if op == "notifications":
        with db() as c:
            # Idle polling must not take a writer lock or fsync a transaction.
            if not c.execute("SELECT 1 FROM jobs WHERE notification='pending' LIMIT 1").fetchone():
                return None
            c.execute("PRAGMA busy_timeout=2000")
            c.execute("BEGIN IMMEDIATE")
            row = c.execute("""SELECT * FROM jobs WHERE notification='pending'
                               AND status IN ('succeeded','failed','cancelled','timed_out',
                                              'interrupted','waiting_answer')
                               ORDER BY ended_at LIMIT 1""").fetchone()
            if not row:
                c.execute("COMMIT")
                return None
            c.execute("""UPDATE jobs SET notification='sending',
                         notification_attempts=notification_attempts+1 WHERE id=?""",
                      (row["id"],))
            c.execute("COMMIT")
            return {"id": row["id"], "channel": row["channel"], "chat_id": row["chat_id"],
                    "session_key": row["session_key"], "message_id": row["message_id"],
                    "status": row["status"], "result": row["final_message"] or row["error"] or "",
                    "verification": row["verification"]}
    if op == "notification_ack":
        with db() as c:
            row = c.execute("SELECT notification,notification_attempts FROM jobs WHERE id=?",
                            (req.get("job_id"),)).fetchone()
            if not row or row["notification"] != "sending":
                raise ValueError("通知状态无效")
            if req.get("sent") is True:
                state = "sent"
            elif row["notification_attempts"] < 3:
                state = "pending"
            else:
                state = "failed"
            c.execute("""UPDATE jobs SET notification=?,notification_at=?,
                         notification_error=? WHERE id=?""",
                      (state, now(), str(req.get("error", ""))[:500], req["job_id"]))
            return {"notification": state}
    owner = require_identity(req)
    with db() as c:
        if op == "conversation_results":
            cutoff = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=7)).isoformat()
            rows = c.execute("""SELECT id, status, prompt, final_message, error,
                                verification, notification_at
                                FROM jobs WHERE owner=? AND session_key=?
                                AND notification='sent' AND notification_at>=?
                                ORDER BY notification_at DESC LIMIT 10""",
                             (owner, req["session_key"], cutoff)).fetchall()
            return [dict(row) for row in reversed(rows)]
        if op in ("status", "result"):
            return visible(own(c, req, req["job_id"]))
        if op == "cancel":
            c.execute("BEGIN IMMEDIATE")
            row = own(c, req, req["job_id"])
            if row["status"] in ("queued", "waiting_answer"):
                c.execute("UPDATE jobs SET status='cancelled',ended_at=?,error=?,notification='pending' WHERE id=?",
                          (now(), "用户取消", row["id"]))
            elif row["status"] == "running":
                c.execute("UPDATE jobs SET status='cancelling' WHERE id=?", (row["id"],))
            c.execute("COMMIT")
            return visible(c.execute("SELECT * FROM jobs WHERE id=?", (row["id"],)).fetchone())
        if op not in ("start", "answer"):
            raise ValueError("未知操作")
        prompt = req.get("prompt")
        if not isinstance(prompt, str) or not prompt.strip() or len(prompt) > MAX_PROMPT:
            raise ValueError("需求为空或过长")
        repo_id = req.get("repo_id")
        read_only = req.get("read_only", False)
        if not isinstance(read_only, bool):
            raise ValueError("read_only 必须是布尔值")
        repo = repo_path(repo_id)
        head, dirty = git_state(repo)
        before_hash = git_fingerprint(repo)
        parent_id = req.get("parent_job_id")
        c.execute("BEGIN IMMEDIATE")
        existing = c.execute("SELECT * FROM jobs WHERE channel=? AND chat_id=? AND message_id=?",
                             (req["channel"], req["chat_id"], req["message_id"])).fetchone()
        if existing:
            c.execute("COMMIT")
            return visible(existing)
        parent = None
        if op == "answer" and not parent_id:
            raise ValueError("回答必须指定父任务")
        if parent_id:
            parent = own(c, req, parent_id)
            if parent["repo_id"] != repo_id or parent["status"] not in ({"waiting_answer"} if op == "answer" else {"succeeded"}):
                raise ValueError("父任务状态或仓库不允许续接")
            if not parent["thread_id"]:
                raise ValueError("父任务没有可用 thread_id")
            if op == "answer":
                prior = c.execute("""SELECT id FROM jobs WHERE parent_id=?
                                     AND status NOT IN ('failed','cancelled','interrupted')
                                     LIMIT 1""", (parent_id,)).fetchone()
                if prior:
                    raise ValueError("此提问已有续接任务：" + prior["id"])
            # The actual session must still be on disk. Codex owns its naming,
            # so refuse if there is no matching session file.
            codex_home = pathlib.Path(os.environ.get("CODEX_HOME", pathlib.Path.home() / ".codex"))
            if not any(parent["thread_id"] in p.name for p in (codex_home / "sessions").rglob("*.jsonl")):
                raise ValueError("父任务会话文件已丢失")
            if dirty != parent["git_after"]:
                raise ValueError("仓库存在父任务结束后的外部改动")
            if parent["git_after_hash"] and before_hash != parent["git_after_hash"]:
                raise ValueError("仓库文件内容在父任务后发生外部改动")
            if op == "answer":
                read_only = parent["sandbox"] == "read-only"
        job_id = uuid.uuid4().hex
        log_dir = str(LOGS / job_id)
        cfg = config()
        c.execute("""INSERT INTO jobs
          (id,owner,channel,chat_id,session_key,message_id,repo_id,prompt,parent_id,
           thread_id,status,created_at,log_dir,model,sandbox,approval,git_head,git_before,git_before_hash)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (job_id, owner, req["channel"], req["chat_id"], req["session_key"],
           req["message_id"], repo_id, prompt, parent_id,
           parent["thread_id"] if parent else None, "queued", now(), log_dir,
           cfg.get("model", "gpt-6-sol"),
           "read-only" if read_only else "danger-full-access", "never", head, dirty, before_hash))
        c.execute("COMMIT")
        return visible(c.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        try:
            line = self.rfile.readline(100000)
            if len(line) >= 100000:
                raise ValueError("请求过大")
            answer = handle(json.loads(line))
            output = {"ok": True, "data": answer}
        except Exception as exc:
            output = {"ok": False, "error": str(exc)}
        self.wfile.write((json.dumps(output, ensure_ascii=False) + "\n").encode())


class Server(socketserver.ThreadingUnixStreamServer):
    daemon_threads = True
    allow_reuse_address = True


def pg_members(pgid):
    found = []
    for entry in pathlib.Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            stat = (entry / "stat").read_text().rsplit(")", 1)[1].split()
            # fields after comm: state(0), ppid(1), pgrp(2)
            if int(stat[2]) == pgid and stat[0] != "Z":
                found.append(int(entry.name))
        except (OSError, ValueError, IndexError):
            pass
    return found


def process_identity(pid):
    try:
        stat = (pathlib.Path("/proc") / str(pid) / "stat").read_text().rsplit(")", 1)[1].split()
        boot = pathlib.Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        return boot + ":" + stat[19]  # Linux starttime field 22 after comm
    except (OSError, ValueError, IndexError):
        return None


def stop_group(pgid):
    if not pgid:
        return True
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and pg_members(pgid):
        time.sleep(.1)
    if pg_members(pgid):
        with contextlib.suppress(ProcessLookupError):
            os.killpg(pgid, signal.SIGKILL)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and pg_members(pgid):
        time.sleep(.1)
    return not pg_members(pgid)


def recover():
    with db() as c:
        # A send accepted by the channel before a crash cannot safely be
        # replayed. Keep it visible as uncertain instead of duplicating it.
        c.execute("UPDATE jobs SET notification='uncertain' WHERE notification='sending'")
        rows = c.execute("SELECT * FROM jobs WHERE status IN ('running','cancelling')").fetchall()
        for row in rows:
            pgid = row["process_group"]
            if pgid and pg_members(pgid):
                if process_identity(pgid) != row["process_start"]:
                    raise RuntimeError("无法证明残留进程组归属，队列暂停: " + row["id"])
                if not stop_group(pgid):
                    raise RuntimeError("残留任务进程未退出，队列暂停: " + row["id"])
            c.execute("UPDATE jobs SET status='interrupted',ended_at=?,error=?,notification='pending' WHERE id=?",
                      (now(), "worker 重启，旧任务未自动重跑", row["id"]))


def prompt_for(row):
    base = ("阅读适用的 AGENTS.md 与项目约定，完成用户需求并运行相关的确定性验证；"
            "最终检查 diff，说明修改文件、验证命令及实际结果。"
            "仓库若已有未提交修改，先检查并保留，区分原有改动与本任务改动；不要擅自 reset、clean 或覆盖。"
            "你已获得执行任务所需命令和操作的默认授权；自行决定常规实现方案、依赖、测试、修复和必要的提交、推送、部署，"
            "按用户目标和项目约定执行，不要为命令权限或普通技术取舍停下询问。"
            "仅在缺少无法推断的业务目标、凭据或外部事实时，最终回复以 NEEDS_USER_INPUT: 开头，写明一个具体问题。"
            "若登录或环境确实无法非交互完成，最终回复以 BLOCKED: 开头并写明原因。"
            "不要声称未经实际执行的测试通过。\n\n用户需求：\n")
    return base + row["prompt"]


def run_job(row, stop):
    job_id = row["id"]
    repo = repo_path(row["repo_id"])
    log_dir = pathlib.Path(row["log_dir"])
    log_dir.mkdir(mode=0o700, exist_ok=False)
    cfg = config()
    codex = cfg.get("codex_bin", "/usr/local/bin/codex")
    if not pathlib.Path(codex).is_absolute() or not os.access(codex, os.X_OK):
        raise ValueError("Codex 二进制路径无效")
    final_path = log_dir / "final.txt"
    argv = [codex, "exec", "-m", row["model"], "-c", 'approval_policy="never"',
            "--json", "-o", str(final_path)]
    if row["parent_id"]:
        argv += ["resume", row["thread_id"], "-"]
    else:
        argv += ["-C", repo, "--sandbox", row["sandbox"], "-"]
    # Resume CLI has no -C or --sandbox. CWD and -c sandbox_mode are passed
    # explicitly; both flags are supported by exec resume --help.
    if row["parent_id"]:
        argv[argv.index("--json"):argv.index("--json")] = ["-c", f'sandbox_mode="{row["sandbox"]}"']
    (log_dir / "argv.json").write_text(json.dumps(argv, ensure_ascii=False))
    os.chmod(log_dir / "argv.json", 0o600)
    started = time.monotonic()
    proc = subprocess.Popen(argv, cwd=repo, stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            start_new_session=True, bufsize=0)
    with ACTIVE_LOCK:
        ACTIVE["pgid"] = proc.pid
    with db() as c:
        c.execute("UPDATE jobs SET process_group=?,process_start=?,progress=? WHERE id=?",
                  (proc.pid, process_identity(proc.pid), "Codex 已启动", job_id))
    proc.stdin.write(prompt_for(row).encode())
    proc.stdin.close()
    sel = selectors.DefaultSelector()
    sel.register(proc.stdout, selectors.EVENT_READ, ("jsonl", log_dir / "events.jsonl"))
    sel.register(proc.stderr, selectors.EVENT_READ, ("stderr", log_dir / "stderr.log"))
    streams = {"jsonl": bytearray(), "stderr": bytearray()}
    sizes = {"jsonl": 0, "stderr": 0}
    saw_complete = False
    saw_failed = False
    saw_error = False
    checks = []
    thread_id = row["thread_id"]
    stop_reason = None
    stop_detail = None
    with (log_dir / "events.jsonl").open("wb") as out, (log_dir / "stderr.log").open("wb") as err:
        files = {"jsonl": out, "stderr": err}
        while sel.get_map() or proc.poll() is None:
            with db() as c:
                state = c.execute("SELECT status FROM jobs WHERE id=?", (job_id,)).fetchone()[0]
            if state == "cancelling":
                stop_reason = "cancelled"
                stop_detail = "用户取消"
            elif stop.is_set():
                stop_reason = "interrupted"
                stop_detail = "worker 停止"
            elif time.monotonic() - started > int(cfg.get("timeout_seconds", 1800)):
                stop_reason = "timed_out"
                stop_detail = "任务超过总时限"
            if stop_reason:
                if not stop_group(proc.pid):
                    raise RuntimeError("任务进程组未能收尾")
                break
            for key, _ in sel.select(.5):
                name, _ = key.data
                chunk = os.read(key.fileobj.fileno(), 65536)
                if not chunk:
                    sel.unregister(key.fileobj)
                    continue
                sizes[name] += len(chunk)
                if sizes[name] > MAX_LOG:
                    stop_reason = "failed"
                    stop_detail = "日志超过大小上限"
                    break
                files[name].write(chunk)
                files[name].flush()
                if name == "jsonl":
                    streams[name] += chunk
                    while b"\n" in streams[name]:
                        raw, _, streams[name] = streams[name].partition(b"\n")
                        try:
                            event = json.loads(raw)
                        except (UnicodeDecodeError, json.JSONDecodeError):
                            saw_error = True
                            continue
                        kind = event.get("type")
                        if kind == "thread.started" and event.get("thread_id"):
                            thread_id = event["thread_id"]
                            with db() as c:
                                c.execute("UPDATE jobs SET thread_id=? WHERE id=?", (thread_id, job_id))
                        elif kind == "turn.completed":
                            saw_complete = True
                        elif kind == "turn.failed":
                            saw_failed = True
                        elif kind == "error":
                            saw_error = True
                        elif kind == "item.completed":
                            item = event.get("item", {})
                            if item.get("type") == "command_execution":
                                command = str(item.get("command", ""))[:500]
                                if re.search(
                                    r"(python3?\s+-m\s+unittest|\bpytest\b|\bgo\s+test\b|"
                                    r"\b(?:pnpm|npm|yarn|bun)\s+(?:run\s+)?(?:test|typecheck|lint|build)\b|"
                                    r"\b(?:cargo|gradle|mvn)\s+test\b)",
                                    command,
                                ):
                                    checks.append({"command": command, "exit_code": item.get("exit_code")})
                                with db() as c:
                                    c.execute("UPDATE jobs SET progress=? WHERE id=?",
                                              (command[:300], job_id))
        proc.wait()
    if pg_members(proc.pid) and not stop_group(proc.pid):
        raise RuntimeError("Codex 子进程组未完全退出")
    with ACTIVE_LOCK:
        if ACTIVE["pgid"] == proc.pid:
            ACTIVE["pgid"] = None
    proc.stdout.close()
    proc.stderr.close()
    if streams["jsonl"]:
        saw_error = True
    final = final_path.read_text(errors="replace") if final_path.exists() else ""
    with contextlib.suppress(Exception):
        head, dirty = git_state(repo)
        after_hash = git_fingerprint(repo)
    status = stop_reason
    error = stop_detail
    if status is None:
        if proc.returncode != 0 or not saw_complete or saw_failed or saw_error:
            status = "failed"
            error = f"Codex 退出码 {proc.returncode}；完整完成事件={saw_complete}；终止错误={saw_failed or saw_error}"
        elif final.lstrip().startswith("NEEDS_USER_INPUT:") or (
            re.match(r"^(请确认|请回答|请提供|请选择|需要你决定|需要您决定)", final.lstrip())
            and re.search(r"[?？]", final[:300])):
            status = "waiting_answer"
        elif final.lstrip().startswith("BLOCKED:"):
            status = "failed"
            error = final[:1000]
        else:
            status = "succeeded"
    if status == "succeeded" and row["sandbox"] == "read-only" and (
        "after_hash" not in locals() or after_hash != row["git_before_hash"]
    ):
        status = "failed"
        error = "只读任务执行期间仓库文件发生变化"
    latest_checks = {x["command"]: x["exit_code"] for x in checks}
    verification = ("not_required" if row["sandbox"] == "read-only" else
                    "passed" if latest_checks and all(code == 0 for code in latest_checks.values()) else
                    "failed" if any(code not in (None, 0) for code in latest_checks.values()) else "unknown")
    with db() as c:
        c.execute("""UPDATE jobs SET status=?,ended_at=?,exit_code=?,error=?,thread_id=?,
                     git_after=?,git_after_hash=?,final_message=?,verification=?,requirement=?,
                     checks_json=?,notification='pending',progress=? WHERE id=?""",
                  (status, now(), proc.returncode, error, thread_id,
                   dirty if "dirty" in locals() else None,
                   after_hash if "after_hash" in locals() else None, final[:10000],
                   verification, "needs_answer" if status == "waiting_answer" else
                   ("reported_complete" if status == "succeeded" else "incomplete"),
                   json.dumps(checks, ensure_ascii=False),
                   "执行结束", job_id))


def worker(stop):
    while not stop.is_set():
        with db() as c:
            c.execute("BEGIN IMMEDIATE")
            row = c.execute("SELECT * FROM jobs WHERE status='queued' ORDER BY created_at,id LIMIT 1").fetchone()
            if row:
                parent = c.execute("SELECT status FROM jobs WHERE id=?", (row["parent_id"],)).fetchone() if row["parent_id"] else None
                if parent and parent["status"] not in ("succeeded", "waiting_answer"):
                    c.execute("UPDATE jobs SET status='failed',ended_at=?,error=?,notification='pending' WHERE id=?",
                              (now(), "父任务未成功，未启动续作", row["id"]))
                    row = None
                else:
                    c.execute("UPDATE jobs SET status='running',started_at=? WHERE id=?", (now(), row["id"]))
            c.execute("COMMIT")
        if not row:
            stop.wait(.5)
            continue
        try:
            repo = repo_path(row["repo_id"])
            head, dirty = git_state(repo)
            before_hash = git_fingerprint(repo)
            if row["parent_id"]:
                if dirty != row["git_before"]:
                    raise ValueError("排队期间仓库发生外部改动")
            elif head != row["git_head"] or dirty != row["git_before"]:
                raise ValueError("排队期间仓库发生外部改动")
            if row["git_before_hash"] and before_hash != row["git_before_hash"]:
                raise ValueError("排队期间仓库文件内容发生外部改动")
            run_job(row, stop)
        except Exception as exc:
            with ACTIVE_LOCK:
                pgid = ACTIVE["pgid"]
            if pgid:
                if not stop_group(pgid):
                    raise RuntimeError("残留任务进程未退出，队列暂停") from exc
                with ACTIVE_LOCK:
                    ACTIVE["pgid"] = None
            with db() as c:
                c.execute("""UPDATE jobs SET status='failed',ended_at=?,error=?,notification='pending'
                             WHERE id=? AND status IN ('running','cancelling')""",
                          (now(), str(exc)[:1000], row["id"]))


def serve():
    global WORKER_THREAD
    init()
    lock_file = (ROOT / "runner.lock").open("a+b")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        raise RuntimeError("已有 runner worker 在运行") from exc
    recover()
    if SOCKET.exists():
        SOCKET.unlink()
    stop = threading.Event()
    thread = threading.Thread(target=worker, args=(stop,), daemon=True)
    WORKER_THREAD = thread
    thread.start()
    with Server(str(SOCKET), Handler) as server:
        os.chmod(SOCKET, 0o600)
        def ending(*_):
            stop.set()
            with ACTIVE_LOCK:
                pgid = ACTIVE["pgid"]
            if pgid:
                stop_group(pgid)
            threading.Thread(target=server.shutdown, daemon=True).start()
        signal.signal(signal.SIGTERM, ending)
        signal.signal(signal.SIGINT, ending)
        server.serve_forever(poll_interval=.5)
    thread.join(timeout=10)
    SOCKET.unlink(missing_ok=True)


def request(payload):
    with socket.socket(socket.AF_UNIX) as s:
        s.settimeout(5)
        s.connect(str(SOCKET))
        s.sendall((json.dumps(payload, ensure_ascii=False) + "\n").encode())
        return json.loads(s.makefile().readline())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["serve", "request", "health"])
    args = parser.parse_args()
    if args.command == "serve":
        serve()
    elif args.command == "health":
        print(json.dumps(request({"op": "health"}), ensure_ascii=False))
    else:
        print(json.dumps(request(json.load(sys.stdin)), ensure_ascii=False))
