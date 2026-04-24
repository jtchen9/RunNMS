"""
Layer 4 tests for experiment registry and northbound experiment snapshot.

Usage:
- Edit TEST_TO_RUN below
- Run this file directly in VS Code

Safety rules:
- Does NOT touch whitelist.
- Only resets KEY_EXPERIMENT_REGISTRY.
- Uses direct function calls, no HTTP, no argparse.
"""

from __future__ import annotations

from datetime import timedelta
import json
from typing import Any, Dict, List

import config
import utility
import m4Commands
import m5Northbound


def _print_title(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def _assert(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


def _reset_experiment_registry() -> None:
    config.r.delete(config.KEY_EXPERIMENT_REGISTRY)


def _dump_experiment_registry(limit: int = 20) -> None:
    print(f"\n[{config.KEY_EXPERIMENT_REGISTRY}]")
    rows = config.r.xrange(config.KEY_EXPERIMENT_REGISTRY, count=limit)
    if not rows:
        print("(empty)")
        return

    for xid, fields in rows:
        print(f"- {xid}: {fields}")


def _registry_rows() -> List[Any]:
    return config.r.xrange(config.KEY_EXPERIMENT_REGISTRY, count=200)


def _dt(offset_sec: int):
    return utility.parse_local_dt(utility.local_ts()) + timedelta(seconds=offset_sec)


def _register_exp(
    *,
    t0_offset_sec: int,
    duration_sec: int,
    item_count: int = 3,
    source: str = "test",
    scenario_name: str = "test_scenario",
) -> Dict[str, Any]:
    t0 = _dt(t0_offset_sec)
    last = t0 + timedelta(seconds=duration_sec)

    return m4Commands._register_experiment_status(
        t0_str=t0.strftime(config.TIME_FMT),
        last_execute_at_str=last.strftime(config.TIME_FMT),
        item_count=item_count,
        source=source,
        scenario_name=scenario_name,
    )


def _print_snapshot() -> Dict[str, Any]:
    snap = m5Northbound._build_experiment_snapshot()
    print("\nSnapshot:")
    print(json.dumps(snap, ensure_ascii=False, indent=2))
    return snap


def test_register_one_experiment() -> None:
    _print_title("TEST: register one experiment into stream")

    _reset_experiment_registry()

    exp = _register_exp(
        t0_offset_sec=60,
        duration_sec=300,
        item_count=5,
        source="load_csv_file",
        scenario_name="one_exp.csv",
    )

    print("\nRegistered:")
    print(json.dumps(exp, ensure_ascii=False, indent=2))

    _dump_experiment_registry()

    rows = _registry_rows()
    _assert(len(rows) == 1, "Expected exactly one experiment row")
    _, fields = rows[0]

    _assert(fields.get("session_id") == exp.get("session_id"), "Expected session_id to match")
    _assert(fields.get("scenario_name") == "one_exp.csv", "Expected scenario_name saved")
    _assert(fields.get("source") == "load_csv_file", "Expected source saved")
    _assert(str(fields.get("item_count")) == "5", "Expected item_count saved as 5")

    print("\nPASS")


def test_register_two_experiments_no_overwrite() -> None:
    _print_title("TEST: register two experiments without overwrite")

    _reset_experiment_registry()

    exp1 = _register_exp(
        t0_offset_sec=60,
        duration_sec=120,
        item_count=3,
        source="load_csv_file",
        scenario_name="first.csv",
    )

    exp2 = _register_exp(
        t0_offset_sec=600,
        duration_sec=120,
        item_count=4,
        source="load_csv_file",
        scenario_name="second.csv",
    )

    print("\nRegistered exp1:")
    print(json.dumps(exp1, ensure_ascii=False, indent=2))
    print("\nRegistered exp2:")
    print(json.dumps(exp2, ensure_ascii=False, indent=2))

    _dump_experiment_registry()

    rows = _registry_rows()
    _assert(len(rows) == 2, "Expected two experiment rows")
    names = [fields.get("scenario_name") for _, fields in rows]

    _assert("first.csv" in names, "Expected first experiment to remain")
    _assert("second.csv" in names, "Expected second experiment to remain")

    print("\nPASS")


def test_snapshot_scheduled() -> None:
    _print_title("TEST: snapshot reports nearest future experiment as scheduled")

    _reset_experiment_registry()

    _register_exp(t0_offset_sec=600, duration_sec=120, scenario_name="later.csv")
    _register_exp(t0_offset_sec=60, duration_sec=120, scenario_name="nearest.csv")

    _dump_experiment_registry()
    snap = _print_snapshot()

    _assert(snap.get("state") == "scheduled", "Expected scheduled state")
    _assert(snap.get("scenario_name") == "nearest.csv", "Expected nearest future experiment")
    _assert(snap.get("next_scheduled_at") is not None, "Expected next_scheduled_at")

    print("\nPASS")


def test_snapshot_running() -> None:
    _print_title("TEST: snapshot reports running experiment")

    _reset_experiment_registry()

    _register_exp(t0_offset_sec=-30, duration_sec=300, scenario_name="running.csv")
    _register_exp(t0_offset_sec=600, duration_sec=120, scenario_name="future.csv")

    _dump_experiment_registry()
    snap = _print_snapshot()

    _assert(snap.get("state") == "running", "Expected running state")
    _assert(snap.get("scenario_name") == "running.csv", "Expected running experiment selected")
    _assert(snap.get("started_at") is not None, "Expected started_at")
    _assert(int(snap.get("elapsed_sec") or 0) >= 0, "Expected non-negative elapsed_sec")

    print("\nPASS")


def test_snapshot_idle_after_end() -> None:
    _print_title("TEST: snapshot reports idle after last experiment ended")

    _reset_experiment_registry()

    _register_exp(t0_offset_sec=-600, duration_sec=120, scenario_name="old.csv")
    _register_exp(t0_offset_sec=-300, duration_sec=60, scenario_name="recent_old.csv")

    _dump_experiment_registry()
    snap = _print_snapshot()

    _assert(snap.get("state") == "idle", "Expected idle state")
    _assert(snap.get("scenario_name") == "recent_old.csv", "Expected latest past experiment")
    _assert(int(snap.get("idle_duration_sec") or 0) >= 0, "Expected non-negative idle_duration_sec")
    _assert(snap.get("next_scheduled_at") is None, "Expected no next_scheduled_at")

    print("\nPASS")


if __name__ == "__main__":
    tests = {
        "test_register_one_experiment": test_register_one_experiment,
        "test_register_two_experiments_no_overwrite": test_register_two_experiments_no_overwrite,
        "test_snapshot_scheduled": test_snapshot_scheduled,
        "test_snapshot_running": test_snapshot_running,
        "test_snapshot_idle_after_end": test_snapshot_idle_after_end,
    }
    TEST_TO_RUN = "test_snapshot_idle_after_end"

    fn = tests.get(TEST_TO_RUN)
    if fn is None:
        raise ValueError(f"Unknown TEST_TO_RUN: {TEST_TO_RUN}")

    fn()
