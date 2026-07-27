from __future__ import annotations

import math
from typing import Any, Dict, List

from .script_model import ScriptRow


def _row_issue(
    row: ScriptRow,
    *,
    code: str,
    message: str,
    suggestion: str,
    **details: Any,
) -> Dict[str, Any]:
    issue: Dict[str, Any] = {
        "level": "error",
        "code": code,
        "row_number": row.row_number,
        "scanner": row.scanner,
        "category": row.category,
        "action": row.action,
        "message": message,
        "suggestion": suggestion,
    }
    issue.update(details)
    return issue


def _is_finite_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def check_command_arguments(
    rows: List[ScriptRow],
    policy: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Validate the public argument contract of every admitted script command.

    The contracts are data-driven through script_policy.json. Vocabulary rules
    decide whether a category/action pair is admitted; this checker validates
    arguments only for those admitted pairs.
    """
    issues: List[Dict[str, Any]] = []

    allowed_by_category = policy.get("allowed_actions_by_category", {}) or {}
    rules_by_category = policy.get("argument_rules_by_category", {}) or {}

    for row in rows:
        allowed_actions = set(allowed_by_category.get(row.category, []) or [])
        if row.action not in allowed_actions:
            # vocabulary_rules.py reports unknown/mismatched commands.
            continue

        category_rules = rules_by_category.get(row.category, {}) or {}
        rule = category_rules.get(row.action)
        if not isinstance(rule, dict):
            issues.append(
                _row_issue(
                    row,
                    code="COMMAND_ARGUMENT_RULE_MISSING",
                    message=(
                        f"no argument rule is configured for admitted command "
                        f"{row.action}."
                    ),
                    suggestion=(
                        "Add this command to argument_rules_by_category in "
                        "CommonCheckers/config/script_policy.json."
                    ),
                )
            )
            continue

        args = row.args or {}
        allowed_keys = set(rule.get("allowed_keys", []) or [])
        required_keys = set(rule.get("required_keys", []) or [])
        numeric_ranges = rule.get("numeric_ranges", {}) or {}

        actual_keys = set(args.keys())
        unknown_keys = sorted(str(key) for key in actual_keys - allowed_keys)

        if not allowed_keys and actual_keys:
            issues.append(
                _row_issue(
                    row,
                    code="COMMAND_ARGS_NOT_ALLOWED",
                    message=(
                        f"{row.action} does not accept arguments, but args_json "
                        f"contains keys: {unknown_keys}."
                    ),
                    suggestion="Use an empty JSON object: {}.",
                    unknown_keys=unknown_keys,
                )
            )
            continue

        if unknown_keys:
            issues.append(
                _row_issue(
                    row,
                    code="COMMAND_ARGS_UNKNOWN_FIELDS",
                    message=(
                        f"{row.action} contains unsupported argument keys: "
                        f"{unknown_keys}."
                    ),
                    suggestion=(
                        f"Use only these keys: {sorted(allowed_keys)}."
                    ),
                    unknown_keys=unknown_keys,
                    allowed_keys=sorted(allowed_keys),
                )
            )

        missing_keys = sorted(
            str(key) for key in required_keys if key not in actual_keys
        )
        if missing_keys:
            issues.append(
                _row_issue(
                    row,
                    code="COMMAND_ARGS_MISSING_REQUIRED",
                    message=(
                        f"{row.action} is missing required argument keys: "
                        f"{missing_keys}."
                    ),
                    suggestion=(
                        f"Provide all required keys: {sorted(required_keys)}."
                    ),
                    missing_keys=missing_keys,
                    required_keys=sorted(required_keys),
                )
            )

        for field_name, range_rule in numeric_ranges.items():
            if field_name not in args:
                continue

            value = args[field_name]
            if not _is_finite_number(value):
                issues.append(
                    _row_issue(
                        row,
                        code="COMMAND_ARG_BAD_TYPE",
                        message=(
                            f"{row.action} argument {field_name} must be a "
                            f"finite JSON number; got {value!r}."
                        ),
                        suggestion=(
                            f"Enter {field_name} as a number without quotes."
                        ),
                        field=field_name,
                        actual_value=value,
                        expected_type="finite number",
                    )
                )
                continue

            numeric_value = float(value)
            minimum = range_rule.get("min_inclusive")
            maximum = range_rule.get("max_exclusive")

            below_minimum = (
                minimum is not None and numeric_value < float(minimum)
            )
            at_or_above_maximum = (
                maximum is not None and numeric_value >= float(maximum)
            )

            if below_minimum or at_or_above_maximum:
                interval = (
                    f"[{float(minimum):g}, {float(maximum):g})"
                    if minimum is not None and maximum is not None
                    else "the configured range"
                )
                issues.append(
                    _row_issue(
                        row,
                        code="COMMAND_ARG_OUT_OF_RANGE",
                        message=(
                            f"{row.action} argument {field_name}={numeric_value:g} "
                            f"is outside the allowed range {interval}."
                        ),
                        suggestion=(
                            f"Choose {field_name} within {interval}."
                        ),
                        field=field_name,
                        actual_value=numeric_value,
                        min_inclusive=minimum,
                        max_exclusive=maximum,
                    )
                )

    return issues
