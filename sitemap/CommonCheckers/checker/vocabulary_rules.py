from __future__ import annotations
from typing import Any, Dict, List
from .script_model import ScriptRow


def _allowed_actions_by_category(policy: Dict[str, Any]) -> Dict[str, set[str]]:
    """
    Build category/action vocabulary.

    Backward compatible:
    - old policy only had allowed_mobility_actions
    - Phase-10a adds allowed_actions_by_category and scan commands
    """
    raw = policy.get("allowed_actions_by_category")
    if isinstance(raw, dict) and raw:
        return {
            str(category): {str(action) for action in (actions or [])}
            for category, actions in raw.items()
        }

    out: Dict[str, set[str]] = {
        "mobility": {str(x) for x in policy.get("allowed_mobility_actions", [])}
    }
    if policy.get("allowed_scan_actions"):
        out["scan"] = {str(x) for x in policy.get("allowed_scan_actions", [])}
    return out


def _blocked_actions_by_category(policy: Dict[str, Any]) -> Dict[str, set[str]]:
    raw = policy.get("blocked_actions_by_category")
    if isinstance(raw, dict) and raw:
        return {
            str(category): {str(action) for action in (actions or [])}
            for category, actions in raw.items()
        }

    return {
        "mobility": {str(x) for x in policy.get("blocked_mobility_actions", [])}
    }


def check_vocabulary(rows: List[ScriptRow], policy: Dict[str, Any]) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []
    allowed_by_category = _allowed_actions_by_category(policy)
    blocked_by_category = _blocked_actions_by_category(policy)
    allowed_categories = set(policy.get("allowed_categories", [])) or set(allowed_by_category.keys())

    for row in rows:
        category = str(row.category or "").strip()
        action = str(row.action or "").strip()

        if category not in allowed_categories:
            issues.append({
                "level": "error",
                "code": "UNKNOWN_CATEGORY",
                "row_number": row.row_number,
                "scanner": row.scanner,
                "category": category,
                "action": action,
                "message": f"unknown or unsupported command category: {category}",
                "suggestion": f"Allowed categories are: {sorted(allowed_categories)}.",
            })
            continue

        blocked = blocked_by_category.get(category, set())
        if action in blocked:
            issues.append({
                "level": "error",
                "code": "BLOCKED_ACTION",
                "row_number": row.row_number,
                "scanner": row.scanner,
                "category": category,
                "action": action,
                "message": f"{action} is not allowed in AutoLab scripts.",
                "suggestion": "Use public semantic script commands instead of low-level/internal commands.",
            })
            continue

        allowed = allowed_by_category.get(category, set())
        if action not in allowed:
            issues.append({
                "level": "error",
                "code": "UNKNOWN_ACTION",
                "row_number": row.row_number,
                "scanner": row.scanner,
                "category": category,
                "action": action,
                "message": f"unknown or unsupported {category} action: {action}",
                "suggestion": f"Allowed {category} actions are: {sorted(allowed)}.",
            })

    return issues
