from __future__ import annotations

import json
import sys
from pathlib import Path

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from t9_mobility_report_intercept import arm_rule  # noqa: E402

SCANNER = "twin-scout-charlie"

RULE = {
    "mode": "patch",
    "match_action": "mobility.turn_move_turn.forward",
    "once": True,
    "patch": {
        "last_exec_status": "failed",
        "last_error_code": "MOVE_EXEC_FAIL",
        "last_error_detail": "V6 injected MOVE_EXEC_FAIL",
        "last_location_result": {
            "ok": False,
            "error": "V6 move exec fail"
        }
    }
}

if __name__ == "__main__":
    out = arm_rule(SCANNER, RULE)
    print(json.dumps({"armed": out, "rule": RULE}, ensure_ascii=False, indent=2))
