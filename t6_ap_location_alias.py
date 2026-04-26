"""
Layer 6 tests: AP location + alias metadata.

Goal:
Verify experiment CSV-style AP metadata rows:
- update AP meta location and alias
- do NOT enqueue commands to AP command stream
- appear correctly in 10-second northbound status snapshot

Safety rules:
- Tests NEVER write to KEY_WHITELIST_SCANNER_META.
- AP must already be whitelisted.
- Tests only reset AP runtime meta/command stream.
- No argparse; edit variables below and run in VS Code.
"""

from __future__ import annotations

import json
from typing import Any, Dict

import config
import utility
import m4Commands
import m5Northbound


AP_SCANNER = "prplOS-fff8c8.lan"
AP_ALIAS = "AP0"

TEST_TO_RUN = "test_ap_meta_update_only"
# Options:
# - test_ap_meta_update_only
# - test_ap_snapshot_alias_location
# - test_ap_location_update_overwrites_previous


def _require_real_whitelist(scanner: str) -> None:
    if not config.r.hexists(config.KEY_WHITELIST_SCANNER_META, scanner):
        raise RuntimeError(
            f"{scanner} is not whitelisted. Do not let tests create whitelist entries."
        )

    raw = config.r.hget(config.KEY_WHITELIST_SCANNER_META, scanner)
    if raw is None:
        raise RuntimeError(f"{scanner} whitelist entry missing")

    if str(raw).strip() in ("1", "true", "True"):
        raise RuntimeError(
            f"{scanner} whitelist entry is corrupted: {raw!r}. Restore whitelist from backup before running tests."
        )


def _print_title(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def _assert(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


def _dump_hash(key: str) -> None:
    print(f"\n[{key}]")
    data = config.r.hgetall(key)
    if not data:
        print("(empty)")
        return
    for k in sorted(data.keys()):
        print(f"{k}: {data[k]}")


def _dump_stream(key: str, count: int = 20) -> None:
    print(f"\n[{key}]")
    rows = config.r.xrange(key, count=count)
    if not rows:
        print("(empty)")
        return
    for xid, fields in rows:
        print(f"- {xid}: {fields}")


def _stream_count(key: str) -> int:
    return int(config.r.xlen(key))


def _reset_ap_runtime(scanner: str) -> None:
    _require_real_whitelist(scanner)

    for k in [
        config.key_scanner_meta(scanner),
        config.key_cmd_stream(scanner),
        config.key_cmdack_stream(scanner),
    ]:
        try:
            config.r.delete(k)
        except Exception:
            pass

    config.r.sadd(config.KEY_REGISTRY, scanner)


def _apply_ap_location(
    scanner: str,
    *,
    alias: str,
    x_m: float,
    y_m: float,
    z_m: float | None = 1.8,
) -> None:
    args: Dict[str, Any] = {
        "x_m": x_m,
        "y_m": y_m,
        "alias": alias,
    }
    if z_m is not None:
        args["z_m"] = z_m

    m4Commands._enqueue_script_or_csv_item(
        scanner=scanner,
        category="ap_meta",
        action="ap.location.update",
        execute_at=utility.local_ts(),
        args=args,
    )


def _get_ap_from_snapshot(scanner: str, alias: str) -> Dict[str, Any]:
    snap = m5Northbound._build_status_snapshot(traffic_events=[])
    print("\nNorthbound snapshot AP list:")
    print(json.dumps(snap.get("aps", []), ensure_ascii=False, indent=2))

    for ap in snap.get("aps", []):
        if ap.get("ap_real_id") == scanner:
            return ap
        if ap.get("ap_id") == alias:
            return ap

    return {}


def _show_ap(scanner: str) -> None:
    _dump_hash(config.key_scanner_meta(scanner))
    _dump_stream(config.key_cmd_stream(scanner))


def test_ap_meta_update_only() -> None:
    _print_title("TEST: AP metadata update only, no AP command enqueue")

    _reset_ap_runtime(AP_SCANNER)

    _apply_ap_location(
        AP_SCANNER,
        alias=AP_ALIAS,
        x_m=2.0,
        y_m=3.5,
        z_m=1.8,
    )

    _show_ap(AP_SCANNER)

    meta = config.r.hgetall(config.key_scanner_meta(AP_SCANNER))

    _assert(meta.get("device_type") == "ap", "Expected device_type=ap")
    _assert(meta.get("ap_alias") == AP_ALIAS, "Expected ap_alias")
    _assert(meta.get("location_mode") == "fixed", "Expected fixed location mode")
    _assert(float(meta.get("location_x_m", "nan")) == 2.0, "Expected x_m=2.0")
    _assert(float(meta.get("location_y_m", "nan")) == 3.5, "Expected y_m=3.5")
    _assert(float(meta.get("location_z_m", "nan")) == 1.8, "Expected z_m=1.8")
    _assert(meta.get("location_source") == "experiment_csv", "Expected location_source=experiment_csv")
    _assert(str(meta.get("location_updated_at") or "").strip() != "", "Expected location_updated_at")

    _assert(_stream_count(config.key_cmd_stream(AP_SCANNER)) == 0, "AP meta row must NOT enqueue AP command")

    print("\nPASS")


def test_ap_snapshot_alias_location() -> None:
    _print_title("TEST: AP alias and location appear in northbound snapshot")

    _reset_ap_runtime(AP_SCANNER)

    _apply_ap_location(
        AP_SCANNER,
        alias=AP_ALIAS,
        x_m=2.0,
        y_m=3.5,
        z_m=1.8,
    )

    ap = _get_ap_from_snapshot(AP_SCANNER, AP_ALIAS)
    _assert(bool(ap), "Expected AP to appear in snapshot")

    print("\nSelected AP:")
    print(json.dumps(ap, ensure_ascii=False, indent=2))

    _assert(ap.get("ap_id") == AP_ALIAS, "Expected ap_id to use alias")
    _assert(ap.get("ap_real_id") == AP_SCANNER, "Expected ap_real_id to preserve real scanner name")
    _assert(ap.get("alias") == AP_ALIAS, "Expected alias field")

    loc = ap.get("location") or {}
    _assert(loc.get("mode") == "fixed", "Expected fixed location")
    _assert(float(loc.get("x")) == 2.0, "Expected location.x=2.0")
    _assert(float(loc.get("y")) == 3.5, "Expected location.y=3.5")
    _assert(float(loc.get("z")) == 1.8, "Expected location.z=1.8")
    _assert(str(loc.get("updated_at") or "").strip() != "", "Expected location updated_at")

    _assert(_stream_count(config.key_cmd_stream(AP_SCANNER)) == 0, "Snapshot test must not enqueue AP command")

    print("\nPASS")


def test_ap_location_update_overwrites_previous() -> None:
    _print_title("TEST: AP location update overwrites previous location")

    _reset_ap_runtime(AP_SCANNER)

    _apply_ap_location(
        AP_SCANNER,
        alias="AP_OLD",
        x_m=1.0,
        y_m=1.0,
        z_m=1.0,
    )

    first_meta = config.r.hgetall(config.key_scanner_meta(AP_SCANNER))
    print("\nFirst meta:")
    print(json.dumps(first_meta, ensure_ascii=False, indent=2))

    _apply_ap_location(
        AP_SCANNER,
        alias=AP_ALIAS,
        x_m=8.0,
        y_m=4.5,
        z_m=2.2,
    )

    _show_ap(AP_SCANNER)

    meta = config.r.hgetall(config.key_scanner_meta(AP_SCANNER))

    _assert(meta.get("ap_alias") == AP_ALIAS, "Expected alias overwritten")
    _assert(float(meta.get("location_x_m", "nan")) == 8.0, "Expected x overwritten")
    _assert(float(meta.get("location_y_m", "nan")) == 4.5, "Expected y overwritten")
    _assert(float(meta.get("location_z_m", "nan")) == 2.2, "Expected z overwritten")

    ap = _get_ap_from_snapshot(AP_SCANNER, AP_ALIAS)
    _assert(ap.get("ap_id") == AP_ALIAS, "Expected snapshot to use new alias")

    loc = ap.get("location") or {}
    _assert(float(loc.get("x")) == 8.0, "Expected snapshot x overwritten")
    _assert(float(loc.get("y")) == 4.5, "Expected snapshot y overwritten")
    _assert(float(loc.get("z")) == 2.2, "Expected snapshot z overwritten")

    _assert(_stream_count(config.key_cmd_stream(AP_SCANNER)) == 0, "AP location update must not enqueue AP command")

    print("\nPASS")


if __name__ == "__main__":
    tests = {
        "test_ap_meta_update_only": test_ap_meta_update_only,
        "test_ap_snapshot_alias_location": test_ap_snapshot_alias_location,
        "test_ap_location_update_overwrites_previous": test_ap_location_update_overwrites_previous,
    }

    fn = tests.get(TEST_TO_RUN)
    if fn is None:
        raise ValueError(f"Unknown TEST_TO_RUN: {TEST_TO_RUN}")

    fn()
