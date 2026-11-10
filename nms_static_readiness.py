"""Registration-time readiness for the specific targets of an accepted CSV."""
import asyncio
import json
import time
import uuid

import config
import utility
from fastapi import HTTPException


async def check_targets(names, t0):
    r = config.r
    now = utility.parse_local_dt(utility.local_ts())
    # Keep time for the existing registration/reset/enqueue steps.
    budget = min(150.0, (t0 - now).total_seconds() - 5)
    if budget <= 0:
        raise HTTPException(409, "Insufficient lead time for startup checks; choose a later t0.")
    deadline = time.monotonic() + budget
    if not r.ping():
        raise HTTPException(503, "Startup failed: Redis unavailable")
    robots, statuses, pending = [], {}, {}
    for name in sorted(set(names)):
        meta = r.hgetall(config.key_scanner_meta(name))
        try:
            age = (now - utility.parse_local_dt(meta['last_seen'])).total_seconds()
        except (KeyError, TypeError, ValueError):
            age = None
        kind = str(meta.get('device_type', '')).lower()
        if age is None or not 0 <= age <= 30 or kind not in ('robot', 'ap'):
            raise HTTPException(409, f"Startup: {name} offline, stale or unknown")
        if kind == 'robot':
            robots.append(name)
        else:
            statuses[name] = 'connected'
    try:
        for name in robots:
            nonce = uuid.uuid4().hex
            tail = r.xrevrange(config.key_cmdack_stream(name), count=1)
            stamp = utility.local_ts()
            xid = r.xadd(config.key_cmd_stream(name), {
                'category':'health', 'action':'health.check.static',
                'execute_at':stamp, 'created_at':stamp,
                'args_json':json.dumps({'request_id':nonce,
                                        'expires_ts':time.time()+max(0,deadline-time.monotonic())})})
            pending[name] = [xid, nonce, tail[0][0] if tail else '0-0']
        while pending:
            if time.monotonic() >= deadline:
                raise HTTPException(409, "Startup check timed out: " + ', '.join(pending)
                                    + ". Experiment not registered; choose a later t0.")
            for name, item in list(pending.items()):
                xid, nonce, cursor = item
                for ack_id, fields in r.xrange(config.key_cmdack_stream(name), min='('+cursor, count=200):
                    item[2] = ack_id
                    if fields.get('cmd_id') != xid:
                        continue
                    try:
                        detail = json.loads(fields.get('detail',''))
                    except (ValueError,TypeError):
                        detail = {}
                    if not (isinstance(detail, dict) and fields.get('status') == 'ok'
                            and detail.get('schema') == 'static_health_v1'
                            and detail.get('request_id') == nonce and detail.get('status') == 'PASS'):
                        reason = detail.get('reason') if isinstance(detail, dict) else ''
                        raise HTTPException(409, f"Startup failed: {name}: {reason or fields.get('detail') or 'invalid reply'}")
                    statuses[name] = 'static PASS'
                    del pending[name]  # normal cmd_ack deletes the command
                    break
            if pending:
                await asyncio.sleep(0.25)  # polling and ACK endpoints remain available
    finally:
        for name,(xid,_,_) in pending.items():
            r.xdel(config.key_cmd_stream(name), xid)
    if (t0 - utility.parse_local_dt(utility.local_ts())).total_seconds() < 5:
        raise HTTPException(409, "Startup finished too close to t0; choose a later t0.")
    return statuses
