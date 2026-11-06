"""Activate the sealed history-memory candidate after explicit user approval.

Preserves a complete stopped database backup, then makes a verified stable
DB+WAL copy after activation for the independent read-only integrity auditor.
"""
from concurrent.futures import ThreadPoolExecutor
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import ssl
import subprocess
import threading
import time
import urllib.parse
import urllib.request

ROOT = Path('/home/cryptoapp/flowsurface/trade-recorder').resolve()
VERIFY = ROOT / 'verification-20261023-history-memory-final-1725'
SERVICE = 'flowsurface-trade-recorder.service'
TIMER = 'flowsurface-trade-recorder-health.timer'
HEALTH = 'flowsurface-trade-recorder-health.service'
OLD = 'cbd3b61824240af7190da09b5b341c21911f7f4a1f84cda415eeeef94b1f8b92'
NEW = '55a488604de3074b29c5e6224f250a06d97aa4205dde759286438035a927b855'
BASE = 'https://100.113.189.20:8080'
CONTEXT = ssl._create_unverified_context()
TOKEN = (ROOT / 'data/.auth_token').read_text().strip()


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def ctl(*args):
    return subprocess.run(['systemctl', '--user', *args], check=True,
                          capture_output=True, text=True).stdout.strip()


def request(endpoint):
    req = urllib.request.Request(BASE + endpoint,
                                 headers={'Authorization': 'Bearer ' + TOKEN})
    return urllib.request.urlopen(req, context=CONTEXT, timeout=55)


def query(endpoint):
    with request(endpoint) as response:
        return json.load(response)


def logs(pid, since):
    return subprocess.run(['journalctl', '--user', '-u', SERVICE, '_PID=' + str(pid),
                           '--since', since, '--no-pager', '-o', 'cat'],
                          check=True, capture_output=True, text=True).stdout


def signature(path):
    info = path.stat()
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def stopped_backup(destination):
    assert ctl('show', SERVICE, '-p', 'MainPID', '--value') == '0'
    destination.mkdir(mode=0o700)
    for name in ['trades.duckdb', 'trades.duckdb.wal']:
        source = ROOT / 'data' / name
        if source.exists():
            before = signature(source)
            shutil.copyfile(source, destination / name)
            assert signature(source) == before
    return {'directory': str(destination), 'captured_while_stopped': True}


def stable_live_copy(destination):
    # DuckDB appends ordinary commits to the WAL between checkpoints. Copy
    # only if the main DB is unchanged throughout and the WAL is unchanged
    # during its entire read. Hash equality proves the copied main DB bytes.
    for attempt in range(5):
        target = destination / ('attempt-' + str(attempt))
        target.mkdir(parents=True, mode=0o700)
        main = ROOT / 'data/trades.duckdb'
        wal = ROOT / 'data/trades.duckdb.wal'
        before = signature(main)
        shutil.copyfile(main, target / main.name)
        if signature(main) != before:
            continue
        copied_hash = digest(target / main.name)
        source_hash = digest(main)
        if copied_hash != source_hash or signature(main) != before:
            continue
        wal_before = signature(wal) if wal.exists() else None
        if wal_before is not None:
            shutil.copyfile(wal, target / wal.name)
        wal_after = signature(wal) if wal.exists() else None
        if signature(main) != before or wal_before != wal_after:
            continue
        return {'directory': str(target), 'main_sha256': copied_hash,
                'main_unchanged_throughout_copy': True, 'wal_stable_during_copy': True,
                'wal_sha256': digest(target / wal.name) if wal_before else None}
    raise RuntimeError('could not obtain a byte-stable DB+WAL copy; production was left running')


def main():
    os.umask(0o077)
    assert digest(ROOT / 'bin/flowsurface-server') == OLD
    candidate = VERIFY / 'flowsurface-server-candidate'
    assert digest(candidate) == NEW
    qualification = json.loads((VERIFY / 'qualification.json').read_text())['fixed']
    assert qualification['running_binary_sha256'] == NEW
    assert all(item.get('status') == 200 for item in qualification['requests'])
    assert qualification['out_of_memory_errors'] == qualification['flush_failures'] == 0
    stamp = datetime.datetime.now(datetime.timezone.utc)
    since = stamp.isoformat()
    work = VERIFY / ('activation-' + stamp.strftime('%Y%m%dT%H%M%SZ'))
    work.mkdir(mode=0o700)
    protected = [ROOT / 'config.toml', ROOT / 'bin/healthcheck.sh', ROOT / 'data/.auth_token',
                 ROOT / 'data/cert.pem', ROOT / 'data/key.pem']
    report = {'authorized_activation': True, 'directory': str(work),
              'candidate_sha256': NEW, 'baseline_sha256': OLD,
              'protected_hashes_before': {str(p.relative_to(ROOT)): digest(p) for p in protected},
              'health_before': query('/status'), 'pairs_before': query('/pairs'),
              'old_pid': ctl('show', SERVICE, '-p', 'MainPID', '--value')}
    assert report['health_before']['feeds_ok']
    shutil.copy2(ROOT / 'bin/flowsurface-server', work / 'flowsurface-server-before')
    assert digest(work / 'flowsurface-server-before') == OLD
    shutil.copy2(ROOT / 'config.toml', work / 'config-before.toml')
    staged = ROOT / 'bin/flowsurface-server.history-memory-new'
    shutil.copy2(candidate, staged)
    staged.chmod(0o700)
    assert digest(staged) == NEW
    timer_active = ctl('show', TIMER, '-p', 'ActiveState', '--value') == 'active'
    monitoring = threading.Event()
    samples = []

    def monitor():
        while not monitoring.wait(2):
            try:
                samples.append({'time_ms': int(time.time() * 1000), 'status': query('/status')})
            except Exception as error:
                samples.append({'time_ms': int(time.time() * 1000), 'error': str(error)})

    stopped = False
    changed = False
    try:
        if timer_active:
            ctl('stop', TIMER)
        ctl('stop', HEALTH)
        report['stop_requested_ms'] = int(time.time() * 1000)
        print('Stopping recorder gracefully and draining accepted trades.', flush=True)
        ctl('stop', SERVICE)
        stopped = True
        old_logs = logs(report['old_pid'], since)
        (work / 'old-shutdown.log').write_text(old_logs)
        report['old_exit_status'] = ctl('show', SERVICE, '-p', 'Result', '-p', 'ExecMainStatus')
        assert 'Shutdown complete.' in old_logs
        assert 'ExecMainStatus=0' in report['old_exit_status'] and 'Result=success' in report['old_exit_status']
        report['before_backup'] = stopped_backup(work / 'before-data')
        os.replace(staged, ROOT / 'bin/flowsurface-server')
        changed = True
        ctl('start', SERVICE)
        stopped = False
        report['new_pid'] = ctl('show', SERVICE, '-p', 'MainPID', '--value')
        assert report['new_pid'] not in ('0', report['old_pid'])
        report['running_binary_sha256'] = digest(Path('/proc') / report['new_pid'] / 'exe')
        assert report['running_binary_sha256'] == NEW
        print('Verified candidate is running; waiting for current-process trades from all three feeds.', flush=True)
        deadline = time.monotonic() + 150
        while time.monotonic() < deadline:
            try:
                status = query('/status')
                if status.get('feeds_ok') and status.get('db_ok') and not status.get('missing_exchanges') and not status.get('stale_pairs'):
                    break
            except (OSError, ValueError):
                pass
            time.sleep(0.5)
        else:
            raise RuntimeError('recorder did not reach healthy state for all configured sources')
        report['healthy_ms'] = int(time.time() * 1000)
        report['health_after_start'] = status
        report['pairs_after_start'] = query('/pairs')
        if timer_active:
            ctl('start', TIMER)
        watcher = threading.Thread(target=monitor, daemon=True)
        watcher.start()
        fresh_from = int(time.time() * 1000)
        history = work / 'history-pages'
        history.mkdir(mode=0o700)
        day = datetime.datetime.now(datetime.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

        def history_request(item):
            days, venue, symbol = item
            start = int((day - datetime.timedelta(days=days)).timestamp() * 1000)
            params = {'venue': venue, 'market': 'linear', 'symbol': symbol,
                      'from': start, 'to': start + 86_399_999, 'limit': 400000}
            began = time.monotonic()
            with request('/trades.arrow?' + urllib.parse.urlencode(params)) as response:
                payload = response.read()
                filename = f'day{days}-{venue}.arrow'
                (history / filename).write_bytes(payload)
                return dict(params, status=response.status, filename=filename, bytes=len(payload),
                            sha256=hashlib.sha256(payload).hexdigest(), seconds=time.monotonic() - began)

        items = [(days, venue, symbol) for days in (1, 7) for venue, symbol in
                 [('binance', 'btcusdt'), ('bybit', 'btcusdt'), ('hyperliquid', 'btcusdc')]]
        with ThreadPoolExecutor(max_workers=3) as pool:
            report['history_requests'] = list(pool.map(history_request, items))
        assert all(item['status'] == 200 and item['bytes'] > 1024 for item in report['history_requests'])
        (history / 'requests.json').write_text(json.dumps(report['history_requests'], indent=2))
        time.sleep(12)
        report['pairs_after'] = query('/pairs')
        report['health_after'] = query('/status')
        before = {p['ticker']: p['latest'] for p in report['pairs_after_start']['pairs']}
        after = {p['ticker']: p['latest'] for p in report['pairs_after']['pairs']}
        assert len(after) == 3 and all(after[ticker] > latest for ticker, latest in before.items())
        fresh_to = min(after.values())
        assert fresh_to > fresh_from
        report['fresh_coverage'] = {}
        for venue, symbol in [('binance', 'btcusdt'), ('bybit', 'btcusdt'), ('hyperliquid', 'btcusdc')]:
            params = {'venue': venue, 'market': 'linear', 'symbol': symbol, 'from': fresh_from, 'to': fresh_to}
            coverage = query('/coverage?' + urllib.parse.urlencode(params))
            assert coverage['complete'] is True
            report['fresh_coverage'][venue] = coverage
        print('Six real history requests passed; all three feeds have advancing durable trades and complete fresh coverage.', flush=True)
        report['after_snapshot'] = stable_live_copy(work / 'after-data')
        report['protected_hashes_after'] = {str(p.relative_to(ROOT)): digest(p) for p in protected}
        assert report['protected_hashes_after'] == report['protected_hashes_before']
        report['service_memory'] = ctl('show', SERVICE, '-p', 'MemoryCurrent', '-p', 'MemoryPeak', '-p', 'MemoryHigh', '-p', 'MemoryMax', '-p', 'NRestarts')
        new_logs = logs(report['new_pid'], since)
        (work / 'new-recorder.log').write_text(new_logs)
        report['oom_errors'] = new_logs.count('Out of Memory Error')
        report['flush_failures'] = new_logs.count('Batch flush failed')
        report['new_log_errors'] = [line for line in re.sub(r'\x1b\[[0-9;]*m', '', new_logs).splitlines() if ' ERROR ' in line]
        assert report['oom_errors'] == report['flush_failures'] == 0
        assert report['health_after']['feeds_ok']
        report['runtime_verification_passed'] = True
    except BaseException as error:
        report['error'] = str(error)
        if stopped and not changed:
            ctl('start', SERVICE)
        raise
    finally:
        monitoring.set()
        if 'watcher' in locals():
            watcher.join(timeout=60)
        report['health_samples'] = samples
        if timer_active:
            ctl('start', TIMER)
        (work / 'activation.json').write_text(json.dumps(report, indent=2))
        print('Activation receipt: ' + str(work / 'activation.json'), flush=True)
    assert samples and all(sample.get('status', {}).get('feeds_ok') for sample in samples)
    print('Production activation and runtime verification passed. Closed snapshots are ready for full data comparison.', flush=True)


if __name__ == '__main__':
    main()
