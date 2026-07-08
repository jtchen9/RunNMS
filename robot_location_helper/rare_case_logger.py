from __future__ import annotations

"""
Sparse rare-case JSONL logger.

Nothing is logged unless at least one rare-case reason exists.

The log is append-only and replay-oriented:
    - input visible tags
    - solver result
    - confidence evidence
    - optional correction decision
    - explicit reason codes

No Redis and no state transitions.
"""

import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


_LOCK = threading.Lock()


def _ts() -> str:
    return datetime.now().strftime(
        "%Y-%m-%d-%H:%M:%S"
    )


class RareCaseLogger:
    def __init__(
        self,
        path: Path | str,
    ) -> None:
        self.path = Path(path)

    def log_if_rare(
        self,
        *,
        reasons: Iterable[str],
        scanner: Optional[str],
        visible_tags: list[Dict[str, Any]],
        location_result: Dict[str, Any],
        correction_decision: Optional[
            Dict[str, Any]
        ] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        reason_list = [
            str(x)
            for x in reasons
            if str(x).strip()
        ]

        reason_list = list(
            dict.fromkeys(reason_list)
        )

        if not reason_list:
            return False

        record = {
            "logged_at": _ts(),

            "scanner": str(scanner or ""),

            "rare_case_reasons":
                reason_list,

            "solver_version":
                location_result.get(
                    "solver_version",
                    "",
                ),

            "visible_tags":
                visible_tags,

            "location_result":
                location_result,

            "correction_decision":
                correction_decision,

            "context":
                context or {},
        }

        self.path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        line = json.dumps(
            record,
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        )

        with _LOCK:
            with self.path.open(
                "a",
                encoding="utf-8",
            ) as f:
                f.write(line + "\n")

        return True
