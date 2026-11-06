"""Compare history bursts in private recorder instances, never production.

The fixture is a closed DuckDB produced by storage::tests::history_memory_pressure_probe.
Both instances receive identical fixture bytes, the production memory budget,
and three real live feeds. All writes stay below the supplied verification root.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import shutil
import ssl
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def ctl(*args):
    return subprocess.run(["systemctl", "--user", *args], check=True,
                          text=True, capture_output=True).stdout.strip()


def get_json(url):
    with urllib.request.urlopen(url, timeout=15,
                                context=ssl._create_unverified_context()) as response:
        return json.load(response)


def qualify(root, binary, fixture, label, port):
    case = root / label
    case.mkdir(mode=0o700)
    data = case / "data"
    data.mkdir(mode=0o700)
    shutil.copyfile(fixture, data / "trades.duckdb")
    fixture_hash = digest(data / "trades.duckdb")
    assert fixture_hash == digest(fixture)
    config = case / "config.toml"
    config.write_text(f'''[network]
bind_address = "127.0.0.1:{port}"
max_requests = 0
[storage]
data_dir = "data"
max_storage_mb = 0
data_retention_hours = 0
memory_limit_mb = 512
threads = 2
flush_interval_ms = 2000
max_buffered_trades = 200000
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
    unit = f"flowsurface-history-verify-{label}-{int(time.time())}"
    report = {"binary_sha256": digest(binary), "fixture_sha256": fixture_hash,
              "unit": unit, "memory_limit_mb": 512, "requests": [], "health": []}
    base = f"http://127.0.0.1:{port}"
    subprocess.run(["systemd-run", "--user", "--collect", "--unit", unit,
                    "--property", f"WorkingDirectory={case}",
                    "--property", "MemoryHigh=768M", "--property", "MemoryMax=1G",
                    "--property", "CPUQuota=100%", "--property", "Nice=10",
                    "--property", "IOWeight=10", "--property", "TimeoutStopSec=90",
                    str(binary), "--config", str(config)], check=True,
                   text=True, capture_output=True)
    monitoring = threading.Event()
    samples = []

    def monitor():
        while not monitoring.wait(1):
            sample = {"time_ms": int(time.time() * 1000)}
            try:
                sample["production"] = get_json("https://100.113.189.20:8080/status")
                sample["private"] = get_json(base + "/status")
            except Exception as error:
                sample["error"] = str(error)
            samples.append(sample)

    try:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            try:
                status = get_json(base + "/status")
                if status.get("feeds_ok") and not status.get("missing_exchanges"):
                    break
            except (OSError, ValueError):
                state = ctl("show", unit, "-p", "ActiveState", "--value")
                if state in ("failed", "inactive"):
                    raise RuntimeError(f"private recorder stopped during startup: {state}")
            time.sleep(0.5)
        else:
            raise RuntimeError("private recorder did not reach healthy three-feed state")
        report["health_before"] = status
        report["pid"] = ctl("show", unit, "-p", "MainPID", "--value")
        report["running_binary_sha256"] = digest(Path("/proc") / report["pid"] / "exe")
        assert report["running_binary_sha256"] == report["binary_sha256"]
        report["pairs_before"] = get_json(base + "/pairs")
        token_path = data / ".auth_token"
        token = token_path.read_text().strip() if token_path.exists() else None

        def request(item):
            round_no, n = item
            venue = ["binance", "bybit", "hyperliquid"][n % 3]
            params = {"venue": venue, "market": "linear",
                      "symbol": "btcusdc" if venue == "hyperliquid" else "btcusdt",
                      "from": 1790000000000, "to": 1790010000000, "limit": 400000}
            req = urllib.request.Request(base + "/trades.arrow?" + urllib.parse.urlencode(params))
            if token:
                req.add_header("Authorization", "Bearer " + token)
            started = time.monotonic()
            try:
                with urllib.request.urlopen(req, timeout=60) as response:
                    payload = response.read()
                    filename = f"round{round_no}-{n}-{venue}.arrow"
                    (case / filename).write_bytes(payload)
                    return {"filename": filename, "status": response.status,
                            "bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest(),
                            "seconds": time.monotonic() - started}
            except urllib.error.HTTPError as error:
                return {"venue": venue, "status": error.code,
                        "body": error.read().decode(), "seconds": time.monotonic() - started}
            except Exception as error:
                return {"venue": venue, "error": str(error), "seconds": time.monotonic() - started}

        watcher = threading.Thread(target=monitor, daemon=True)
        watcher.start()
        with ThreadPoolExecutor(max_workers=8) as pool:
            for round_no in range(3 if label == "fixed" else 1):
                report["requests"].extend(pool.map(request, [(round_no, n) for n in range(8)]))
        time.sleep(3)
        report["pairs_after"] = get_json(base + "/pairs")
        report["health_after"] = get_json(base + "/status")
        report["memory"] = ctl("show", unit, "-p", "MemoryPeak", "-p", "MemoryCurrent",
                               "-p", "MemoryHigh", "-p", "MemoryMax", "-p", "NRestarts")
    except Exception as error:
        report["error"] = str(error)
    finally:
        monitoring.set()
        if "watcher" in locals():
            watcher.join(timeout=35)
        report["health"] = samples
        logs = subprocess.run(["journalctl", "--user", "-u", unit, "--no-pager", "-o", "cat"],
                              capture_output=True, text=True, check=True).stdout
        (case / "recorder.log").write_text(logs)
        report["out_of_memory_errors"] = logs.count("Out of Memory Error")
        report["flush_failures"] = logs.count("Batch flush failed")
        subprocess.run(["systemctl", "--user", "stop", unit], capture_output=True, text=True)
        report["clean_shutdown"] = "Shutdown complete." in subprocess.run(
            ["journalctl", "--user", "-u", unit, "--no-pager", "-o", "cat"],
            capture_output=True, text=True, check=True).stdout
        (case / "report.json").write_text(json.dumps(report, indent=2))
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--skip-baseline", action="store_true")
    args = parser.parse_args()
    root = args.root.resolve()
    assert root.parent == Path("/home/cryptoapp/flowsurface/trade-recorder")
    assert root.name.startswith("verification-")
    assert root.is_dir()
    production_pid = ctl("show", "flowsurface-trade-recorder.service", "-p", "MainPID", "--value")
    production_hash = digest(Path("/proc") / production_pid / "exe")
    assert production_hash == digest(args.baseline)
    results = {}
    cases = [("baseline", args.baseline, 28180), ("fixed", args.candidate, 28181)]
    if args.skip_baseline:
        results["baseline"] = json.loads((root / "baseline" / "report.json").read_text())
        cases = cases[1:]
    for label, binary, port in cases:
        results[label] = qualify(root, binary.resolve(), args.fixture.resolve(), label, port)
        print(json.dumps({"case": label,
                          "statuses": [r.get("status", r.get("error")) for r in results[label]["requests"]],
                          "oom_errors": results[label]["out_of_memory_errors"],
                          "flush_failures": results[label]["flush_failures"],
                          "error": results[label].get("error")}), flush=True)
    assert ctl("show", "flowsurface-trade-recorder.service", "-p", "MainPID", "--value") == production_pid
    assert digest(Path("/proc") / production_pid / "exe") == production_hash
    results["production_pid_unchanged"] = True
    results["production_binary_unchanged"] = True
    (root / "qualification.json").write_text(json.dumps(results, indent=2))
    fixed = results["fixed"]
    assert not fixed.get("error"), fixed.get("error")
    assert len(fixed["requests"]) == 24
    assert all(request.get("status") == 200 for request in fixed["requests"])
    assert fixed["out_of_memory_errors"] == fixed["flush_failures"] == 0
    assert fixed["health_after"]["feeds_ok"] and fixed["clean_shutdown"]
    before = {pair["ticker"]: pair["latest"] for pair in fixed["pairs_before"]["pairs"]}
    after = {pair["ticker"]: pair["latest"] for pair in fixed["pairs_after"]["pairs"]}
    assert len(before) == len(after) == 3
    assert all(after[ticker] > latest for ticker, latest in before.items())
    assert fixed["health"]
    assert all(sample.get("production", {}).get("feeds_ok") for sample in fixed["health"])
    assert all(sample.get("private", {}).get("feeds_ok") for sample in fixed["health"])
    print("Private history qualification passed; production recorder unchanged.")


if __name__ == "__main__":
    main()
