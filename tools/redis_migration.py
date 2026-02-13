#!/usr/bin/env python3
"""
Migrate whitelist from:

OLD:
  nms:registry:whitelist_name2mac
  HASH(scanner -> mac)

NEW:
  nms:registry:whitelist_scanner_meta
  HASH(scanner -> json(meta))

LLM link is initialized to:
  https://chatgpt.com/

Safe to run once when Redis only contains old-format whitelist.
"""

from __future__ import annotations

import json
import redis
from datetime import datetime

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

REDIS_URL = "redis://localhost:6379/0"

OLD_KEY = "nms:registry:whitelist_name2mac"
NEW_KEY = "nms:registry:whitelist_scanner_meta"

WHITELIST_SCHEMA_VERSION = 1
GENERIC_LLM_LINK = "https://chatgpt.com/"

TIME_FMT = "%Y-%m-%d-%H:%M:%S"

def local_ts() -> str:
    return datetime.now().strftime(TIME_FMT)

# ---------------------------------------------------------------------
# Migration
# ---------------------------------------------------------------------

def main():
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

    if not r.exists(OLD_KEY):
        print(f"[ERR] Old key not found: {OLD_KEY}")
        return

    old_map = r.hgetall(OLD_KEY) or {}
    if not old_map:
        print(f"[WARN] Old key exists but empty: {OLD_KEY}")
        return

    now = local_ts()

    new_payload = {}

    for scanner, mac in sorted(old_map.items()):
        scanner = (scanner or "").strip()
        mac = (mac or "").strip().lower()

        if not scanner or not mac:
            continue

        meta = {
            "schema_version": WHITELIST_SCHEMA_VERSION,
            "scanner": scanner,
            "mac": mac,
            "llm_weblink": GENERIC_LLM_LINK,
            "comment": "",
            "updated_at": now,
        }

        new_payload[scanner] = json.dumps(meta, ensure_ascii=False)

    # Write new key
    r.hset(NEW_KEY, mapping=new_payload)

    print(f"[OK] Migrated {len(new_payload)} entries to {NEW_KEY}")

    # Backup old key (rename instead of delete)
    backup_key = f"{OLD_KEY}__backup__{now.replace('-', '').replace(':', '')}"
    r.rename(OLD_KEY, backup_key)

    print(f"[OK] Old key renamed to {backup_key}")
    print("[DONE] Migration completed successfully.")


if __name__ == "__main__":
    main()
