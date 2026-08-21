from __future__ import annotations

import csv
import json
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .script_model import ScriptRow


def _issue(row: ScriptRow, code: str, message: str, suggestion: str) -> Dict[str, Any]:
    return {
        "level": "error",
        "code": code,
        "row_number": row.row_number,
        "scanner": row.scanner,
        "action": row.action,
        "message": message,
        "suggestion": suggestion,
    }


def _definitions(macro_policy: Dict[str, Any]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    by_token: Dict[str, Dict[str, Any]] = {}
    by_action: Dict[str, Dict[str, Any]] = {}
    for action, raw_cfg in dict(macro_policy.get("macros", {}) or {}).items():
        cfg = dict(raw_cfg or {})
        token = str(cfg.get("launch_constant") or "").strip().upper()
        if not token:
            continue
        item = {
            "token": token,
            "macro_action": str(action),
            "x_m": float(cfg["start_x_m"]),
            "y_m": float(cfg["start_y_m"]),
        }
        by_token[token] = item
        by_action[str(action)] = item
    return by_token, by_action


def _token(value: Any, known_tokens: set[str]) -> str:
    if not isinstance(value, str):
        return ""
    candidate = value.strip().upper()
    return candidate if candidate in known_tokens else ""


def _numeric_launch_matches(args: Dict[str, Any], definition: Dict[str, Any]) -> bool:
    try:
        return (
            abs(float(args["x_m"]) - float(definition["x_m"])) <= 1e-9
            and abs(float(args["y_m"]) - float(definition["y_m"])) <= 1e-9
        )
    except (KeyError, TypeError, ValueError):
        return False


def resolve_reserved_launch_locations(
    rows: List[ScriptRow],
    macro_policy: Dict[str, Any],
) -> Tuple[List[ScriptRow], List[Dict[str, Any]], int]:
    """
    Validate reserved staging constants, then return numeric row copies.

    The source XLSM CSV may contain:
        {"x_m":"IN2OUT", "y_m":"IN2OUT"}

    Downstream validation receives numeric coordinates. Numeric coordinates at
    the exact launch point are also admitted so the normalized CSV can pass the
    identical checker again during NMS registration.
    """
    by_token, by_action = _definitions(macro_policy)
    known_tokens = set(by_token)
    issues: List[Dict[str, Any]] = []
    resolved_by_row: Dict[int, ScriptRow] = {}
    symbolic_token_by_row: Dict[int, str] = {}

    for row in rows:
        args = dict(row.args or {})
        if row.category == "mobility" and row.action == "mobility.move":
            x_token = _token(args.get("x_m"), known_tokens)
            y_token = _token(args.get("y_m"), known_tokens)
            if x_token or y_token:
                if not x_token or not y_token or x_token != y_token:
                    issues.append(_issue(
                        row,
                        "RESERVED_LAUNCH_PAIR_REQUIRED",
                        "x_m and y_m must use the same reserved launch constant.",
                        "Use IN2OUT in both cells or OUT2IN in both cells.",
                    ))
                else:
                    definition = by_token[x_token]
                    args["x_m"] = definition["x_m"]
                    args["y_m"] = definition["y_m"]
                    symbolic_token_by_row[row.row_number] = x_token
        resolved_by_row[row.row_number] = replace(row, args=args)

    ordered = sorted(rows, key=lambda r: (r.t_offset_sec, r.row_number))
    last_mobility_by_scanner: Dict[str, ScriptRow] = {}
    pending_symbolic_by_scanner: Dict[str, ScriptRow] = {}

    for source_row in ordered:
        if source_row.category != "mobility":
            continue

        previous_pending = pending_symbolic_by_scanner.get(source_row.scanner)
        source_token = symbolic_token_by_row.get(source_row.row_number, "")

        if previous_pending is not None:
            pending_token = symbolic_token_by_row[previous_pending.row_number]
            expected_action = by_token[pending_token]["macro_action"]
            if source_row.action != expected_action:
                issues.append(_issue(
                    previous_pending,
                    "RESERVED_LAUNCH_NOT_FOLLOWED_BY_MACRO",
                    f"{pending_token} staging move is followed by {source_row.action}, not {expected_action}.",
                    f"Make {expected_action} the next mobility command for {source_row.scanner}.",
                ))
            pending_symbolic_by_scanner.pop(source_row.scanner, None)

        if source_row.action in by_action:
            definition = by_action[source_row.action]
            previous = last_mobility_by_scanner.get(source_row.scanner)
            previous_resolved = resolved_by_row.get(previous.row_number) if previous else None
            preceding_matches = bool(
                previous_resolved
                and previous_resolved.action == "mobility.move"
                and (
                    symbolic_token_by_row.get(previous_resolved.row_number) == definition["token"]
                    or _numeric_launch_matches(previous_resolved.args, definition)
                )
            )
            if not preceding_matches:
                issues.append(_issue(
                    source_row,
                    "MACRO_REQUIRES_PRECEDING_RESERVED_MOVE",
                    f"{source_row.action} must be preceded by a mobility.move to {definition['token']} for the same robot, with no intervening mobility command.",
                    f"Add mobility.move with x_m={definition['token']} and y_m={definition['token']} immediately before this macro.",
                ))

        if source_token:
            pending_symbolic_by_scanner[source_row.scanner] = source_row

        last_mobility_by_scanner[source_row.scanner] = source_row

    for scanner, pending in pending_symbolic_by_scanner.items():
        token = symbolic_token_by_row[pending.row_number]
        expected_action = by_token[token]["macro_action"]
        issues.append(_issue(
            pending,
            "RESERVED_LAUNCH_NOT_FOLLOWED_BY_MACRO",
            f"{token} staging move has no following {expected_action} mobility command for {scanner}.",
            f"Add {expected_action} as the next mobility command for this robot.",
        ))

    resolved_rows = [resolved_by_row[row.row_number] for row in rows]
    return resolved_rows, issues, len(symbolic_token_by_row)


def write_normalized_script_csv(path: str | Path, rows: List[ScriptRow]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".normalizing.tmp")
    with temporary.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=["scanner", "t_offset_sec", "category", "action", "args_json"],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({
                "scanner": row.scanner,
                "t_offset_sec": row.t_offset_sec,
                "category": row.category,
                "action": row.action,
                "args_json": json.dumps(row.args or {}, ensure_ascii=False, separators=(",", ":")),
            })
    temporary.replace(path)
