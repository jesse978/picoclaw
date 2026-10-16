import datetime as dt
import sqlite3
import time
import unittest

import test_runner as fixture

runner = fixture.runner


class ConversationRepairTest(unittest.TestCase):
    setUp = fixture.RunnerTest.setUp
    start = fixture.RunnerTest.start

    def test_only_sent_results_in_same_conversation_are_imported(self):
        jobs = [self.start("task " + str(i), message_id="m" + str(i)) for i in range(5)]
        recent = runner.now()
        old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=8)).isoformat()
        with runner.db() as c:
            for job in jobs:
                c.execute("UPDATE jobs SET status='succeeded',notification='sent',"
                          "notification_at=?,final_message='done' WHERE id=?", (recent, job["id"]))
            c.execute("UPDATE jobs SET notification='pending' WHERE id=?", (jobs[1]["id"],))
            c.execute("UPDATE jobs SET session_key='other' WHERE id=?", (jobs[2]["id"],))
            c.execute("UPDATE jobs SET owner='weixin:other' WHERE id=?", (jobs[3]["id"],))
            c.execute("UPDATE jobs SET notification_at=? WHERE id=?", (old, jobs[4]["id"]))
        result = runner.handle({**self.id, "op": "conversation_results"})
        self.assertEqual([j["id"] for j in result], [jobs[0]["id"]])
        self.assertEqual(result[0]["prompt"], "task 0")
        with self.assertRaises(ValueError):
            runner.handle({**self.id, "op": "conversation_results", "chat_id": "unauthorized"})
        with runner.db() as c:
            self.assertEqual(c.execute("SELECT notification FROM jobs WHERE id=?", (jobs[1]["id"],)).fetchone()[0], "pending")

    def test_idle_notification_poll_does_not_wait_for_writer(self):
        with runner.db() as writer:
            writer.execute("BEGIN IMMEDIATE")
            started = time.monotonic()
            self.assertIsNone(runner.handle({"op": "notifications"}))
            self.assertLess(time.monotonic() - started, 1)
            writer.execute("ROLLBACK")

    def test_database_context_closes_connection(self):
        with runner.db() as c:
            self.assertEqual(c.execute("SELECT 1").fetchone()[0], 1)
        with self.assertRaises(sqlite3.ProgrammingError):
            c.execute("SELECT 1")


if __name__ == "__main__":
    unittest.main()
