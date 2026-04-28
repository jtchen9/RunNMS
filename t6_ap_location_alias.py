"""
Layer 6 tests: AP location + alias metadata.

Goal:
Verify experiment CSV-style AP metadata rows:
- update AP meta using location_json
- update AP alias when provided
- do NOT enqueue commands to AP command stream
- appear correctly in 10-second northbound status snapshot

Current AP meta schema:
- ap_alias: "AP2"
- location_json:
    {
      "mode": "fixed",
      "x": 2.0,
      "y": 3.5,
      "z": 1.8,
      "source": "experiment_csv",
      "updated_at": "YYYY-MM-DD-HH:MM:SS"
    }

Safety rules:
- Tests NEVER write to KEY_WHITELIST_SCANNER_META.
- AP must already be whitelisted.
- Tests reset only this AP's runtime meta/command streams.
- No argparse; edit variables below and run in VS Code.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

import config
import utility
import m4Commands
import m5Northbound


# ============================================================
# Editable test selection
# ============================================================

AP_SCANNER = "prplOS-fff8c8.lan"
AP_ALIAS = "AP2"

TEST_TO_RUN = "test_ap_meta_update_only"
# Options:
# - test_ap_meta_update_only
# - test_ap_snapshot_alias_location
# - test_ap_location_update_overwrites_previous
# - test_ap_location_without_alias_preserves_existing_alias
# - test_ap_meta_row_does_not_create_old_location_fields


# ============================================================
# Safety / utility helpers
# ============================================================

OLD_AP_LOCATION_FIELDS = [
    "location_x_m",
    "location_y_m",
    "location_z_m",
    "location_source",
    "location_mode",
    "location_updated_at",
]


def _require_real_whitelist(scanner: str) -> None:
    """
    Never create or overwrite whitelist entries from tests.
    The production whitelist hash stores structured metadata.
    """
    if not config.r.hexists(config.KEY_WHITELIST_SCANNER_META, scanner):
        raise RuntimeError(
            f"{scanner} is not whitelisted. Do not let tests create whitelist entries."
        )

    raw = config.r.hget(config.KEY_WHITELIST_SCANNER_META, scanner)
    if raw is None:
        raise RuntimeError(f"{scanner} whitelist entry missing")

    if str(raw).strip() in ("1", "true", "True"):
        raise RuntimeError(
            f"{scanner} whitelist entry is corrupted: {raw!r}. "
            "Restore whitelist from backup before running tests."
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
    """
    Reset only this AP's runtime/debug state.

    This intentionally deletes scanner meta for this AP because the test verifies
    AP meta creation/update. It does NOT touch whitelist.
    """
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

    # Registry is runtime state, not protected config.
    config.r.sadd(config.KEY_REGISTRY, scanner)


def _apply_ap_location(
    scanner: str,
    *,
    alias: Optional[str],
    x: float,
    y: float,
    z: Optional[float] = 1.8,
) -> None:
    """
    Simulate one CSV experiment row:

    scanner,t_offset_sec,category,action,args_json
    <ap>,0,ap_meta,ap.location.update,{...}
    """
    args: Dict[str, Any] = {
        "x_m": x,
        "y_m": y,
    }
    if z is not None:
        args["z_m"] = z
    if alias is not None:
        args["alias"] = alias

    m4Commands._enqueue_script_or_csv_item(
        scanner=scanner,
        category="ap_meta",
        action="ap.location.update",
        execute_at=utility.local_ts(),
        args=args,
    )


def _load_meta(scanner: str) -> Dict[str, Any]:
    return config.r.hgetall(config.key_scanner_meta(scanner)) or {}


def _load_location_json(meta: Dict[str, Any]) -> Dict[str, Any]:
    raw = meta.get("location_json") or ""
    try:
        obj = json.loads(raw)
    except Exception as e:
        raise AssertionError(f"location_json is not valid JSON: {raw!r}, error={e}") from e

    if not isinstance(obj, dict):
        raise AssertionError(f"location_json must decode to dict, got {type(obj)}")

    return obj


def _assert_location(
    loc: Dict[str, Any],
    *,
    x: float,
    y: float,
    z: Optional[float],
    source: str = "experiment_csv",
) -> None:
    _assert(loc.get("mode") == "fixed", "Expected location.mode=fixed")
    _assert(float(loc.get("x")) == float(x), f"Expected location.x={x}")
    _assert(float(loc.get("y")) == float(y), f"Expected location.y={y}")

    if z is None:
        _assert(loc.get("z") is None, "Expected location.z=None")
    else:
        _assert(float(loc.get("z")) == float(z), f"Expected location.z={z}")

    _assert(loc.get("source") == source, f"Expected location.source={source}")
    _assert(str(loc.get("updated_at") or "").strip() != "", "Expected location.updated_at")


def _assert_no_old_location_fields(meta: Dict[str, Any]) -> None:
    leftovers = [k for k in OLD_AP_LOCATION_FIELDS if k in meta]
    _assert(not leftovers, f"Old split AP location fields should not exist: {leftovers}")


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


# ============================================================
# Tests
# ============================================================


def test_ap_meta_update_only() -> None:
    """
    AP metadata row should:
    - create/update ap_alias
    - create/update location_json
    - not create old split location fields
    - not enqueue command to AP command stream
    """
    _print_title("TEST: AP metadata update only, location_json schema")

    _reset_ap_runtime(AP_SCANNER)

    _apply_ap_location(
        AP_SCANNER,
        alias=AP_ALIAS,
        x=2.0,
        y=3.5,
        z=1.8,
    )

    _show_ap(AP_SCANNER)

    meta = _load_meta(AP_SCANNER)
    loc = _load_location_json(meta)

    _assert(meta.get("device_type") == "ap", "Expected device_type=ap")
    _assert(meta.get("ap_alias") == AP_ALIAS, "Expected ap_alias")
    _assert("location_json" in meta, "Expected location_json")
    _assert_location(loc, x=2.0, y=3.5, z=1.8)
    _assert_no_old_location_fields(meta)

    _assert(
        _stream_count(config.key_cmd_stream(AP_SCANNER)) == 0,
        "AP meta row must NOT enqueue AP command",
    )

    print("\nPASS")


def test_ap_snapshot_alias_location() -> None:
    """
    Northbound 10-second snapshot should report:
    - ap_id as alias
    - ap_real_id as real hostname
    - location object from location_json
    """
    _print_title("TEST: AP alias and location_json appear in northbound snapshot")

    _reset_ap_runtime(AP_SCANNER)

    _apply_ap_location(
        AP_SCANNER,
        alias=AP_ALIAS,
        x=2.0,
        y=3.5,
        z=1.8,
    )

    ap = _get_ap_from_snapshot(AP_SCANNER, AP_ALIAS)
    _assert(bool(ap), "Expected AP to appear in snapshot")

    print("\nSelected AP:")
    print(json.dumps(ap, ensure_ascii=False, indent=2))

    _assert(ap.get("ap_id") == AP_ALIAS, "Expected ap_id to use alias")
    _assert(ap.get("ap_real_id") == AP_SCANNER, "Expected ap_real_id to preserve real scanner name")
    _assert(ap.get("alias") == AP_ALIAS, "Expected alias field")

    loc = ap.get("location") or {}
    _assert_location(loc, x=2.0, y=3.5, z=1.8)

    _assert(
        _stream_count(config.key_cmd_stream(AP_SCANNER)) == 0,
        "Snapshot test must not enqueue AP command",
    )

    print("\nPASS")


def test_ap_location_update_overwrites_previous() -> None:
    """
    AP location is update-based:
    - later experiment location_json overwrites previous location_json
    - alias can also update
    """
    _print_title("TEST: AP location_json update overwrites previous location")

    _reset_ap_runtime(AP_SCANNER)

    _apply_ap_location(
        AP_SCANNER,
        alias="AP_OLD",
        x=1.0,
        y=1.0,
        z=1.0,
    )

    first_meta = _load_meta(AP_SCANNER)
    print("\nFirst meta:")
    print(json.dumps(first_meta, ensure_ascii=False, indent=2))
    first_loc = _load_location_json(first_meta)
    _assert(first_meta.get("ap_alias") == "AP_OLD", "Expected first alias")
    _assert_location(first_loc, x=1.0, y=1.0, z=1.0)

    _apply_ap_location(
        AP_SCANNER,
        alias=AP_ALIAS,
        x=8.0,
        y=4.5,
        z=2.2,
    )

    _show_ap(AP_SCANNER)

    meta = _load_meta(AP_SCANNER)
    loc = _load_location_json(meta)

    _assert(meta.get("ap_alias") == AP_ALIAS, "Expected alias overwritten")
    _assert_location(loc, x=8.0, y=4.5, z=2.2)
    _assert_no_old_location_fields(meta)

    ap = _get_ap_from_snapshot(AP_SCANNER, AP_ALIAS)
    _assert(ap.get("ap_id") == AP_ALIAS, "Expected snapshot to use new alias")

    snap_loc = ap.get("location") or {}
    _assert_location(snap_loc, x=8.0, y=4.5, z=2.2)

    _assert(
        _stream_count(config.key_cmd_stream(AP_SCANNER)) == 0,
        "AP location update must not enqueue AP command",
    )

    print("\nPASS")


def test_ap_location_without_alias_preserves_existing_alias() -> None:
    """
    If CSV row omits alias, existing ap_alias should not be cleared.
    Only location_json should update.
    """
    _print_title("TEST: AP location update without alias preserves existing alias")

    _reset_ap_runtime(AP_SCANNER)

    _apply_ap_location(
        AP_SCANNER,
        alias=AP_ALIAS,
        x=2.0,
        y=3.5,
        z=1.8,
    )

    _apply_ap_location(
        AP_SCANNER,
        alias=None,
        x=9.0,
        y=9.5,
        z=2.5,
    )

    _show_ap(AP_SCANNER)

    meta = _load_meta(AP_SCANNER)
    loc = _load_location_json(meta)

    _assert(meta.get("ap_alias") == AP_ALIAS, "Expected alias preserved when alias omitted")
    _assert_location(loc, x=9.0, y=9.5, z=2.5)
    _assert_no_old_location_fields(meta)

    ap = _get_ap_from_snapshot(AP_SCANNER, AP_ALIAS)
    _assert(ap.get("ap_id") == AP_ALIAS, "Expected snapshot to still use preserved alias")
    _assert_location(ap.get("location") or {}, x=9.0, y=9.5, z=2.5)

    print("\nPASS")


def test_ap_meta_row_does_not_create_old_location_fields() -> None:
    """
    Dedicated regression test:
    AP location update must create only location_json,
    not old split location_* fields.
    """
    _print_title("TEST: AP metadata update does not create old split location fields")

    _reset_ap_runtime(AP_SCANNER)

    _apply_ap_location(
        AP_SCANNER,
        alias=AP_ALIAS,
        x=2.0,
        y=3.5,
        z=1.8,
    )

    meta = _load_meta(AP_SCANNER)
    _show_ap(AP_SCANNER)

    _assert("location_json" in meta, "Expected location_json")
    _assert_no_old_location_fields(meta)

    print("\nPASS")


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    tests = {
        "test_ap_meta_update_only": test_ap_meta_update_only,
        "test_ap_snapshot_alias_location": test_ap_snapshot_alias_location,
        "test_ap_location_update_overwrites_previous": test_ap_location_update_overwrites_previous,
        "test_ap_location_without_alias_preserves_existing_alias": test_ap_location_without_alias_preserves_existing_alias,
        "test_ap_meta_row_does_not_create_old_location_fields": test_ap_meta_row_does_not_create_old_location_fields,
    }

    fn = tests.get(TEST_TO_RUN)
    if fn is None:
        raise ValueError(f"Unknown TEST_TO_RUN: {TEST_TO_RUN}")

    fn()
