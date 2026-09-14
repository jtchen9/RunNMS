#!/usr/bin/env python3
"""Run from NMS host: python tools/nms_health_check.py. No experiment required."""
import argparse
from datetime import datetime
import json
from pathlib import Path
import sys
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def connected(meta, now, fmt, max_age):
    try:
        age = (now - datetime.strptime(meta.get("last_seen", ""), fmt)).total_seconds()
        return 0 <= age <= max_age, age
    except (ValueError, TypeError):
        return False, None


def parse_camera_ack(fields, request_id):
    try:
        data = json.loads(fields.get("detail", ""))
        if data.get("schema") != "camera_health_v1" or data.get("request_id") != request_id:
            raise ValueError("unexpected camera result")
        rows = []
        for role in ("front", "rear"):
            row = data.get("cameras", {}).get(role, {})
            status = row.get("status", "UNKNOWN")
            if status not in ("PASS", "FAIL", "UNKNOWN"):
                status = "UNKNOWN"
            rows.append((role, status, str(row.get("detail", "missing result"))))
        return rows
    except (ValueError, TypeError, AttributeError):
        detail = str(fields.get("detail", "invalid acknowledgement"))[:300]
        return [(role, "UNKNOWN", detail) for role in ("front", "rear")]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--connected-seconds", type=int, default=30,
                        help="last-seen window for robots/APs (default: 30)")
    parser.add_argument("--timeout", type=int, default=90,
                        help="camera response deadline per robot (default: 90 seconds)")
    args = parser.parse_args()
    if args.connected_seconds <= 0 or args.timeout < 60:
        parser.error("connected-seconds must be positive; timeout must be >=60")
    import config
    import redis
    client = redis.Redis.from_url(config.REDIS_URL, decode_responses=True,
                                  socket_connect_timeout=3, socket_timeout=3)
    pending = {}
    failed = False
    try:
        started = time.monotonic()
        if not client.ping():
            raise RuntimeError("PING did not succeed")
        print(f"Redis PASS ({(time.monotonic()-started)*1000:.0f} ms)", flush=True)
        now = datetime.now()
        robots = []
        for name in sorted(client.smembers(config.KEY_REGISTRY)):
            meta = client.hgetall(config.key_scanner_meta(name))
            yes, age = connected(meta, now, config.TIME_FMT, args.connected_seconds)
            if not yes:
                continue
            kind = meta.get("device_type", "unknown")
            print(f"Connected {kind:7} {name}  last seen {age:.0f}s ago", flush=True)
            if kind == "robot":
                robots.append(name)
        if not robots:
            print("Cameras NOT TESTED: no recently connected robots")
        # Only enqueue camera health commands, never mobility commands.
        for name in robots:
            request_id = uuid.uuid4().hex
            stamp = datetime.now().strftime(config.TIME_FMT)
            # Start reading after the current ACK tail; command ID + nonce also match.
            tail = client.xrevrange(config.key_cmdack_stream(name), count=1)
            cursor = tail[0][0] if tail else "0-0"
            xid = client.xadd(config.key_cmd_stream(name), {
                "category": "health", "action": "health.check.cameras",
                "execute_at": stamp, "created_at": stamp,
                "args_json": json.dumps({"request_id": request_id,
                                         "expires_ts": time.time() + args.timeout})})
            pending[name] = [xid, request_id, cursor, time.monotonic()+args.timeout]
        while pending:
            for name, item in list(pending.items()):
                xid, nonce, cursor, deadline = item
                entries = client.xrange(config.key_cmdack_stream(name), min="("+cursor, count=200)
                match = None
                for ack_id, fields in entries:
                    item[2] = ack_id
                    if fields.get("cmd_id") == xid:
                        match = fields
                        break
                if match is not None:
                    for role, status, detail in parse_camera_ack(match, nonce):
                        print(f"Camera {status:7} {name} {role}: {' '.join(detail.split())}", flush=True)
                        failed |= status != "PASS"
                    client.xdel(config.key_cmd_stream(name), xid)
                    del pending[name]
                elif time.monotonic() >= deadline:
                    print(f"Camera UNKNOWN {name}: no acknowledgement before timeout", flush=True)
                    failed = True
                    client.xdel(config.key_cmd_stream(name), xid)
                    del pending[name]
            if pending:
                time.sleep(0.5)
        return 1 if failed else 0
    except KeyboardInterrupt:
        print("Check interrupted")
        return 130
    except Exception as exc:
        print(f"Health check FAIL: {type(exc).__name__}: {exc}")
        return 2
    finally:
        for name, (xid, _, _, _) in pending.items():
            try:
                client.xdel(config.key_cmd_stream(name), xid)
            except Exception:
                pass
        client.close()


if __name__ == "__main__":
    sys.exit(main())
