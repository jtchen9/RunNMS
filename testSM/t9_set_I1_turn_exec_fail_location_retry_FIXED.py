import os

from t9_mobility_report_intercept import (
    DEFAULT_SCANNER,
    arm_rule,
    get_intercept_rule,
    get_enable_value,
)

scanner = os.environ.get("SCANNER", DEFAULT_SCANNER)

rule = {
    "mode": "patch",
    # I1 is a TURN test, so this must match mobility.turn, not forward.
    "match_action": "mobility.turn",
    "once": True,
    "patch": {
        "last_exec_status": "failed",
        "last_error_code": "TURN_EXEC_FAIL",
        "last_error_detail": "I1 debug injected TURN_EXEC_FAIL recoverable turn failure",
        "last_location_result": {
            "ok": False,
            "error": "I1 debug injected TURN_EXEC_FAIL recoverable turn failure",
        },
    },
}

info = arm_rule(scanner, rule)

print("I1 TURN_EXEC_FAIL armed")
print("scanner =", info["scanner"])
print("enable_key =", info["enable_key"])
print("enable_value =", get_enable_value(scanner))
print("rule_key =", info["rule_key"])
print("rule =", get_intercept_rule(scanner))
print("Next matching mobility.turn report will be patched to TURN_EXEC_FAIL.")
print("Expected: NMS should issue mobility.report.location first.")
