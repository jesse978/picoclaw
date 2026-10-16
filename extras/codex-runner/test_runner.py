import importlib.util
import json
import os
import pathlib
import subprocess
import tempfile
import time
import unittest

spec = importlib.util.spec_from_file_location("runner", pathlib.Path(__file__).with_name("runner.py"))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class RunnerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = pathlib.Path(self.tmp.name)
        runner.ROOT = root / "state"
        runner.CONFIG = runner.ROOT / "config.json"
        runner.DB = runner.ROOT / "state.sqlite3"
        runner.SOCKET = runner.ROOT / "runner.sock"
        runner.LOGS = runner.ROOT / "logs"
        runner.init()
        self.repo = root / "repo"
        self.repo.mkdir()
        subprocess.run(["git", "init", "-q", str(self.repo)], check=True)
        subprocess.run(["git", "-C", str(self.repo), "config", "user.email", "test@example.com"], check=True)
        subprocess.run(["git", "-C", str(self.repo), "config", "user.name", "Test"], check=True)
        (self.repo / "README").write_text("test\n")
        subprocess.run(["git", "-C", str(self.repo), "add", "."], check=True)
        subprocess.run(["git", "-C", str(self.repo), "commit", "-qm", "init"], check=True)
        self.fake = root / "fake-codex"
        self.fake.write_text("""#!/usr/bin/env python3
import json, pathlib, sys, time
a=sys.argv; text=sys.stdin.read()
if 'slow' in text: time.sleep(30)
if 'long' in text: time.sleep(62)
thread='test-thread'
print(json.dumps({'type':'thread.started','thread_id':thread}), flush=True)
print(json.dumps({'type':'turn.started'}), flush=True)
if 'noise' in text: print('x'*5000, flush=True)
if 'model_fail' in text:
    print(json.dumps({'type':'turn.failed'}), flush=True)
    sys.exit(2)
if 'change' in text:
    pathlib.Path('changed.txt').write_text('done\\n')
if 'question' in text:
    msg='NEEDS_USER_INPUT: choose A or B?'
elif 'blocked' in text:
    msg='BLOCKED: permission denied outside repository'
else:
    msg='Completed task.'
pathlib.Path(a[a.index('-o')+1]).write_text(msg)
print(json.dumps({'type':'turn.completed'}), flush=True)
""")
        self.fake.chmod(0o700)
        runner.CONFIG.write_text(json.dumps({"codex_bin": str(self.fake), "model": "gpt-6-sol",
                                            "timeout_seconds": 2, "channels": ["weixin"],
                                            "allow_chat_ids": ["admin"], "repo_roots": [str(root)],
                                            "repos": {"test": str(self.repo)}}))
        self.id = {"channel": "weixin", "chat_id": "admin", "session_key": "weixin:admin",
                   "message_id": "m1"}

    def start(self, prompt, **extra):
        return runner.handle({**self.id, "op": "start", "repo_id": "test",
                              "prompt": prompt, **extra})

    def test_idempotent_work_and_dirty_baseline(self):
        one = self.start("change")
        self.assertEqual(one["id"], self.start("change")["id"])
        with runner.db() as c:
            self.assertEqual(c.execute("SELECT sandbox FROM jobs WHERE id=?", (one["id"],)).fetchone()[0],
                             "danger-full-access")
        row = self.run_one(one["id"])
        self.assertEqual(row["status"], "succeeded")
        self.assertEqual(row["thread_id"], "test-thread")
        argv = json.loads((pathlib.Path(row["log_dir"]) / "argv.json").read_text())
        self.assertEqual(argv[argv.index("--sandbox") + 1], "danger-full-access")
        self.assertIn('approval_policy="never"', argv)
        self.assertTrue((self.repo / "changed.txt").exists())
        two = self.start("another", message_id="m2")
        self.assertEqual(two["status"], "queued")
        with runner.db() as c:
            self.assertTrue(c.execute("SELECT git_before FROM jobs WHERE id=?", (two["id"],)).fetchone()[0])

    def test_question_and_session_ownership(self):
        one = self.start("question")
        row = self.run_one(one["id"])
        self.assertEqual(row["status"], "waiting_answer")
        self.assertEqual(row["requirement"], "needs_answer")
        with self.assertRaisesRegex(ValueError, "不属于"):
            runner.handle({**self.id, "session_key": "another", "op": "status", "job_id": one["id"]})
        # Real Codex session files are checked before resume; the fake has none.
        with self.assertRaisesRegex(ValueError, "会话文件"):
            runner.handle({**self.id, "op": "answer", "message_id": "m2",
                           "repo_id": "test", "parent_job_id": one["id"], "prompt": "A"})
        old_home = os.environ.get("CODEX_HOME")
        codex_home = pathlib.Path(self.tmp.name) / "codex"
        (codex_home / "sessions").mkdir(parents=True)
        (codex_home / "sessions" / "test-thread.jsonl").write_text("{}\n")
        os.environ["CODEX_HOME"] = str(codex_home)
        self.addCleanup(lambda: os.environ.__setitem__("CODEX_HOME", old_home)
                        if old_home is not None else os.environ.pop("CODEX_HOME", None))
        answer = runner.handle({**self.id, "op": "answer", "message_id": "m2",
                                "repo_id": "test", "parent_job_id": one["id"], "prompt": "A"})
        self.assertEqual(answer["status"], "queued")
        with self.assertRaisesRegex(ValueError, "已有续接"):
            runner.handle({**self.id, "op": "answer", "message_id": "m3",
                           "repo_id": "test", "parent_job_id": one["id"], "prompt": "B"})

    def test_rejects_unknown_repo_and_shell_text(self):
        with self.assertRaisesRegex(ValueError, "未知"):
            self.start("x", repo_id="../../etc")
        one = self.start("quote '\" $(touch /tmp/codex-runner-injected)\nline")
        row = self.run_one(one["id"])
        self.assertEqual(row["status"], "succeeded")
        self.assertFalse(pathlib.Path("/tmp/codex-runner-injected").exists())

    def test_cancel_and_timeout(self):
        one = self.start("slow")
        row = self.run_one(one["id"])
        self.assertEqual(row["status"], "timed_out")
        self.id["message_id"] = "m2"
        two = self.start("normal")
        cancelled = runner.handle({**self.id, "op": "cancel", "job_id": two["id"]})
        self.assertEqual(cancelled["status"], "cancelled")

    def test_external_content_change_with_same_git_status(self):
        (self.repo / "README").write_text("changed content\n")
        one = self.start("normal", read_only=True)
        first_status = runner.git_state(str(self.repo))[1]
        (self.repo / "README").write_text("another content\n")
        self.assertEqual(first_status, runner.git_state(str(self.repo))[1])
        row = self.run_one(one["id"])
        self.assertEqual(row["status"], "failed")
        self.assertIn("外部改动", row["error"])

    def test_restart_reconciliation_kills_owned_group(self):
        one = self.start("normal")
        child = subprocess.Popen(["sleep", "30"], start_new_session=True)
        self.addCleanup(lambda: child.kill() if child.poll() is None else None)
        with runner.db() as c:
            c.execute("UPDATE jobs SET status='running',process_group=?,process_start=? WHERE id=?",
                      (child.pid, runner.process_identity(child.pid), one["id"]))
        runner.recover()
        child.wait(timeout=3)
        with runner.db() as c:
            row = c.execute("SELECT status,notification FROM jobs WHERE id=?", (one["id"],)).fetchone()
        self.assertEqual(row["status"], "interrupted")
        self.assertEqual(row["notification"], "pending")

    def test_model_failure_and_output_cap(self):
        one = self.start("model_fail")
        self.assertEqual(self.run_one(one["id"])["status"], "failed")
        self.id["message_id"] = "m2"
        old = runner.MAX_LOG
        runner.MAX_LOG = 500
        self.addCleanup(setattr, runner, "MAX_LOG", old)
        two = self.start("noise")
        row = self.run_one(two["id"])
        self.assertEqual(row["status"], "failed")
        self.assertIn("日志超过", row["error"])

    def test_permission_block_is_not_success(self):
        one = self.start("blocked")
        row = self.run_one(one["id"])
        self.assertEqual(row["status"], "failed")
        self.assertIn("permission denied", row["error"])

    def test_cancel_running_process_group(self):
        import threading
        cfg = json.loads(runner.CONFIG.read_text())
        cfg["timeout_seconds"] = 90
        runner.CONFIG.write_text(json.dumps(cfg))
        one = self.start("slow")
        stop = threading.Event()
        thread = threading.Thread(target=runner.worker, args=(stop,), daemon=True)
        thread.start()
        deadline = time.monotonic() + 3
        pgid = None
        while time.monotonic() < deadline:
            with runner.db() as c:
                row = c.execute("SELECT status,process_group FROM jobs WHERE id=?", (one["id"],)).fetchone()
            if row["status"] == "running" and row["process_group"]:
                pgid = row["process_group"]
                break
            time.sleep(.05)
        self.assertIsNotNone(pgid)
        runner.handle({**self.id, "op": "cancel", "job_id": one["id"]})
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            with runner.db() as c:
                row = c.execute("SELECT status FROM jobs WHERE id=?", (one["id"],)).fetchone()
            if row["status"] == "cancelled":
                break
            time.sleep(.05)
        stop.set()
        thread.join(2)
        self.assertEqual(row["status"], "cancelled")
        self.assertEqual(runner.pg_members(pgid), [])

    def test_cancel_waiting_question(self):
        one = self.start("question")
        self.assertEqual(self.run_one(one["id"])["status"], "waiting_answer")
        result = runner.handle({**self.id, "op": "cancel", "job_id": one["id"]})
        self.assertEqual(result["status"], "cancelled")
        with self.assertRaisesRegex(ValueError, "不允许续接"):
            runner.handle({**self.id, "op": "answer", "message_id": "m2",
                           "repo_id": "test", "parent_job_id": one["id"], "prompt": "A"})

    def test_job_runs_past_gateway_timeout(self):
        import threading
        cfg = json.loads(runner.CONFIG.read_text())
        cfg["timeout_seconds"] = 90
        runner.CONFIG.write_text(json.dumps(cfg))
        began = time.monotonic()
        one = self.start("long")
        self.assertLess(time.monotonic() - began, 3)
        stop = threading.Event()
        thread = threading.Thread(target=runner.worker, args=(stop,), daemon=True)
        thread.start()
        time.sleep(61)
        with runner.db() as c:
            state = c.execute("SELECT status FROM jobs WHERE id=?", (one["id"],)).fetchone()[0]
        self.assertEqual(state, "running")
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            with runner.db() as c:
                state = c.execute("SELECT status FROM jobs WHERE id=?", (one["id"],)).fetchone()[0]
            if state == "succeeded":
                break
            time.sleep(.1)
        stop.set()
        thread.join(2)
        self.assertEqual(state, "succeeded")

    def run_one(self, job_id):
        import threading
        stop = threading.Event()
        t = threading.Thread(target=runner.worker, args=(stop,), daemon=True)
        t.start()
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            with runner.db() as c:
                row = c.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row["status"] in runner.TERMINAL:
                stop.set()
                t.join(2)
                return row
            time.sleep(.05)
        stop.set()
        self.fail("job did not finish")


if __name__ == "__main__":
    unittest.main()
