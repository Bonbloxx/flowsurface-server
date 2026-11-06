#!/bin/sh
set -u

status_url="${1:?status URL is required}"

payload="$(
    curl --silent --show-error --insecure \
        --connect-timeout 5 --max-time 10 "$status_url" 2>/dev/null
)" || payload=""

decision="$(printf '%s' "$payload" | python3 -c '
import json, sys
try:
    status = json.load(sys.stdin)
    healthy = status.get("status") == "ok" and status.get("db_ok") is True and status.get("feeds_ok") is True
    recovering = (status.get("status") == "degraded"
        and status.get("recovery_in_progress") is True
        and status.get("db_ok") is True
        and status.get("resolved_feeds_ok") is True
        and status.get("feeds_ok") is False
        and isinstance(status.get("missing_exchanges"), list)
        and bool(status["missing_exchanges"])
        and status.get("stale_pairs") == [])
    print("ok" if healthy else "recovering" if recovering else "restart")
except (ValueError, AttributeError, TypeError):
    print("restart")
')" || decision="restart"

case "$decision" in
    ok) exit 0 ;;
    recovering)
        logger --tag flowsurface-trade-recorder-health \
            "configured venue discovery is retrying; preserving healthy recording"
        exit 0
        ;;
esac

logger --tag flowsurface-trade-recorder-health \
    "recorder status was unavailable or degraded; requesting an isolated service restart"
systemctl --user restart flowsurface-trade-recorder.service
exit 1
