import os

from t9_mobility_report_intercept import (
    DEFAULT_SCANNER,
    arm_rule,
)

scanner = os.environ.get("SCANNER", DEFAULT_SCANNER)

rule = {
    "mode": "patch",
    "match_action": "mobility.turn_move_turn.forward",
    "once": True,
    "patch": {
        "last_exec_status": "failed",
        "last_error_code": "TURN_EXEC_FAIL",
        "last_error_detail": "debug injected TURN_EXEC_FAIL recoverable turn failure",
        "last_location_result": {
            "ok": False,
            "error": "debug injected TURN_EXEC_FAIL recoverable turn failure",
        },
    },
}

info = arm_rule(scanner, rule)

print("I1 TURN_EXEC_FAIL armed")
print("scanner =", info["scanner"])
print("enable_key =", info["enable_key"])
print("rule_key =", info["rule_key"])
print("Next matching mobility.turn_move_turn.forward report will be patched to TURN_EXEC_FAIL.")
print("Expected: NMS should issue mobility.report.location and recover when location succeeds.")
