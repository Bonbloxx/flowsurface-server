"""Run the actual monitor with isolated command stubs, without touching a service."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class HealthcheckTests(unittest.TestCase):
    def check_status(self, payload, restart, curl_exit=0):
        with tempfile.TemporaryDirectory(prefix="recorder-health-test-") as directory:
            root = Path(directory)
            for name, body in {
                "curl": 'printf "%s" "$TEST_PAYLOAD"; exit "$TEST_CURL_EXIT"',
                "logger": "exit 0",
                "systemctl": 'printf "%s\\n" "$*" >> "$TEST_RESTART_LOG"',
            }.items():
                path = root / name
                path.write_text("#!/bin/sh\n" + body + "\n")
                path.chmod(0o700)
            record = root / "restarts"
            env = dict(os.environ, PATH=str(root) + os.pathsep + os.environ["PATH"],
                       TEST_PAYLOAD=payload if isinstance(payload, str) else json.dumps(payload),
                       TEST_CURL_EXIT=str(curl_exit), TEST_RESTART_LOG=str(record))
            result = subprocess.run(["sh", str(Path(__file__).with_name("healthcheck.sh")),
                                     "http://127.0.0.1:1/status"], env=env, capture_output=True)
            self.assertEqual(result.returncode, int(restart), result.stderr.decode())
            self.assertEqual(record.exists(), restart)
            if restart:
                self.assertEqual(record.read_text().strip(),
                                 "--user restart flowsurface-trade-recorder.service")

    def test_healthy(self):
        self.check_status({"status": "ok", "db_ok": True, "feeds_ok": True}, False)

    def test_503_with_managed_recovery_preserves_healthy_feeds(self):
        self.check_status(self.recovering(), False)

    @staticmethod
    def recovering():
        return {"status": "degraded", "db_ok": True, "feeds_ok": False,
                "resolved_feeds_ok": True, "missing_exchanges": ["Hyperliquid Linear"],
                "recovery_in_progress": True, "stale_pairs": []}

    def test_stale_database_failure_or_dead_worker_still_restart(self):
        for overrides in ({"db_ok": False}, {"resolved_feeds_ok": False},
                          {"recovery_in_progress": False},
                          {"stale_pairs": [{"ticker": "BybitLinear:btcusdt"}]},
                          {"missing_exchanges": []}):
            with self.subTest(overrides=overrides):
                self.check_status(self.recovering() | overrides, True)

    def test_unavailable_or_malformed_status_restarts(self):
        for payload in ("", "not JSON", "null", "[]", '{"status":"ok"}'):
            with self.subTest(payload=payload):
                self.check_status(payload, True)
        self.check_status(self.recovering(), True, curl_exit=7)


if __name__ == "__main__":
    unittest.main()
