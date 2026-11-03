"""NMS poll freshness and bounded hold policy. A missing poll is not a Wi-Fi diagnosis."""
import time
import config
import utility


def poll_fresh(scanner):
    raw = utility._hget(config.key_scanner_meta(scanner), "last_poll", "")
    try:
        age = (utility.parse_local_dt(utility.local_ts()) -
               utility.parse_local_dt(raw)).total_seconds()
    except (ValueError, TypeError, AttributeError):
        return False
    return 0 <= age <= config.MOBILITY_POLL_STALE_SEC


def disposition(scanner, time_key, field):
    """One fixed deadline per episode. A poll cannot revive an expired hold."""
    now = time.time()
    deadline = config.r.hget(time_key, field)
    raw = utility._hget(config.key_scanner_meta(scanner), "last_poll", "")
    try:
        last = utility.parse_local_dt(raw).timestamp()
    except (TypeError, ValueError, AttributeError):
        last = None
    if deadline and now >= float(deadline):
        if last is None or last >= float(deadline) or not poll_fresh(scanner):
            return "expired"
    if poll_fresh(scanner):
        if deadline:
            config.r.hdel(time_key, field)
        return "ready"
    if not deadline:
        # First observation of this outage starts the grace period. hsetnx
        # keeps scheduler and S1 from extending the same outage twice.
        config.r.hsetnx(time_key, field, str(now + config.MOBILITY_LINK_GRACE_SEC))
    return "wait"
