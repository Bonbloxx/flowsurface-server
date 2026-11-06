"""Isolated native recorder probe: force Hyperliquid discovery 502s, then release.

Uses temporary storage, loopback ports and a local CONNECT proxy. Never changes
the installed service, its configuration, credentials or database.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
import re
from pathlib import Path
import select
import signal
import socket
import socketserver
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request


class Proxy(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), Tunnel)
        self.release = threading.Event()
        self.failures = 0
        self.count_lock = threading.Lock()


class Tunnel(socketserver.StreamRequestHandler):
    def handle(self):
        self.connection.settimeout(30)
        request = self.rfile.readline(8192).decode("ascii").split()
        if len(request) != 3 or request[0] != "CONNECT":
            return
        while self.rfile.readline(8192) not in (b"\r\n", b"\n", b""):
            pass
        host, port = request[1].rsplit(":", 1)
        if host == "api.hyperliquid.xyz" and not self.server.release.is_set():
            with self.server.count_lock:
                self.server.failures += 1
            self.wfile.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
            self.wfile.flush()
            return
        with socket.create_connection((host, int(port)), timeout=15) as remote:
            self.wfile.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            self.wfile.flush()
            ends = (self.connection, remote)
            while True:
                ready, _, _ = select.select(ends, [], [], 30)
                if not ready:
                    return
                for source in ready:
                    data = source.recv(65536)
                    if not data:
                        return
                    (remote if source is self.connection else self.connection).sendall(data)


def json_get(base, endpoint):
    # Bypass ambient system proxy for the isolated loopback API.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        response = opener.open(base + endpoint, timeout=10)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        return response.code, json.load(response)


def wait_for(process, base, predicate, timeout=90):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError("isolated recorder exited: " + str(process.returncode))
        try:
            last = json_get(base, "/status")
            if predicate(*last):
                return last
        except (OSError, ValueError):
            pass
        time.sleep(0.5)
    raise AssertionError("condition timed out; last status=" + repr(last))


def latest(base):
    _, payload = json_get(base, "/pairs")
    return {pair["ticker"]: pair["latest"] for pair in payload["pairs"]}


def run(binary, report_path, expect_bug):
    report = {"started_utc": datetime.now(timezone.utc).isoformat(),
              "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
              "expect_bug": expect_bug, "passed": False}
    with tempfile.TemporaryDirectory(prefix="recorder-discovery-probe-") as directory:
        root = Path(directory)
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            api_port = listener.getsockname()[1]
        base = "http://127.0.0.1:" + str(api_port)
        config = root / "config.toml"
        config.write_text(f'''[network]
bind_address = "127.0.0.1:{api_port}"
max_requests = 0
[storage]
data_dir = "data"
memory_limit_mb = 256
threads = 2
data_retention_hours = 0
max_storage_mb = 0
flush_interval_ms = 200
[pairs]
base_assets = ["BTC"]
discovery_mode = false
[whitelist.binance]
linear = ["USDT"]
[whitelist.bybit]
linear = ["USDT"]
[whitelist.hyperliquid]
linear = ["USDC"]
''')
        proxy = Proxy()
        thread = threading.Thread(target=proxy.serve_forever, daemon=True)
        thread.start()
        proxy_url = "http://127.0.0.1:" + str(proxy.server_address[1])
        env = dict(os.environ, HTTPS_PROXY=proxy_url, HTTP_PROXY=proxy_url,
                   https_proxy=proxy_url, http_proxy=proxy_url,
                   NO_PROXY="127.0.0.1,localhost", no_proxy="127.0.0.1,localhost",
                   RUST_LOG="flowsurface_server=info,flowsurface_exchange=warn")
        # No inherited service token or application data path enters this probe.
        env.pop("AUTH_TOKEN", None)
        env.pop("FLOWSURFACE_DATA_PATH", None)
        log_path = root / "recorder.log"
        process = None
        try:
            with log_path.open("w") as log:
                process = subprocess.Popen([str(binary), "--config", str(config)],
                                           cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT)
                if expect_bug:
                    status = wait_for(process, base, lambda code, body: code == 200
                                      and body.get("feeds_ok") is True)
                    assert len(latest(base)) == 2, "baseline unexpectedly subscribed missing venue"
                    assert proxy.failures >= 1, "fault was not injected"
                    report.update(bug_reproduced=True, false_healthy_status=status,
                                  injected_502s=proxy.failures)
                else:
                    status = wait_for(process, base, lambda code, body: code == 503
                                      and body.get("resolved_feeds_ok") is True
                                      and body.get("recovery_in_progress") is True
                                      and body.get("missing_exchanges") == ["Hyperliquid Linear"])
                    before = latest(base)
                    assert len(before) == 2, before
                    initial_failures = proxy.failures
                    start_missing = int(time.time() * 1000)
                    time.sleep(12)
                    during = latest(base)
                    assert all(during[key] > timestamp for key, timestamp in before.items()), during
                    assert proxy.failures > initial_failures, "no automatic retry while fault remained"
                    code, during_status = json_get(base, "/status")
                    assert code == 503 and during_status["resolved_feeds_ok"], during_status
                    proxy.release.set()
                    recovered = wait_for(process, base, lambda code, body: code == 200
                                         and body.get("feeds_ok") is True
                                         and body.get("missing_exchanges") == [])
                    after = latest(base)
                    assert len(after) == 3 and all(value is not None for value in after.values()), after
                    assert process.poll() is None, "recovery restarted the probe"
                    time.sleep(5)
                    capture_to = next(value for ticker, value in latest(base).items()
                                      if ticker.startswith("HyperliquidLinear:"))
                    _, coverage = json_get(base, "/coverage?venue=hyperliquid&market=linear"
                        "&symbol=btcusdc&from=" + str(start_missing) + "&to=" + str(capture_to))
                    assert not coverage["complete"] and coverage["segments"], coverage
                    assert coverage["segments"][0]["from"] > start_missing, coverage
                    _, fresh = json_get(base, "/coverage?venue=hyperliquid&market=linear"
                        "&symbol=btcusdc&from=" + str(coverage["segments"][0]["from"])
                        + "&to=" + str(capture_to))
                    assert fresh["complete"], fresh
                    report.update(degraded_status=status, during_status=during_status,
                                  recovered_status=recovered, healthy_feeds_before=before,
                                  healthy_feeds_during=during, all_feeds_after=after,
                                  injected_502s=proxy.failures, process_pid=process.pid,
                                  old_gap_preserved=coverage, recovered_coverage=fresh)
                process.send_signal(signal.SIGTERM)
                process.wait(timeout=30)
            assert process.returncode == 0, "graceful shutdown failed"
            log_text = re.sub(r"\x1b\[[0-9;]*m", "", log_path.read_text())
            assert "Shutdown complete." in log_text, "pipeline did not drain"
            if not expect_bug:
                assert log_text.count("Subscribed to trade stream exchange=Hyperliquid Linear") == 1
                assert log_text.count("Subscribed to trade stream exchange=Binance Linear") == 1
                assert log_text.count("Subscribed to trade stream exchange=Bybit Linear") == 1
            report["graceful_drain_passed"] = True
            report["passed"] = True
        except Exception as error:
            report["error"] = str(error)
            raise
        finally:
            if process and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            proxy.shutdown()
            proxy.server_close()
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(json.dumps(report, indent=2) + "\n")
            if log_path.exists():
                report_path.with_suffix(".log").write_bytes(log_path.read_bytes())
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--expect-bug", action="store_true")
    args = parser.parse_args()
    run(args.binary.resolve(), args.report.resolve(), args.expect_bug)
