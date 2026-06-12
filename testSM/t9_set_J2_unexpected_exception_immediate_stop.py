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
        "last_error_code": "UNEXPECTED_EXCEPTION",
        "last_error_detail": "debug injected UNEXPECTED_EXCEPTION immediate stop",
        "last_location_result": {
            "ok": False,
            "error": "debug injected UNEXPECTED_EXCEPTION immediate stop",
        },
    },
}

info = arm_rule(scanner, rule)

print("J2 UNEXPECTED_EXCEPTION armed")
print("scanner =", info["scanner"])
print("enable_key =", info["enable_key"])
print("rule_key =", info["rule_key"])
print("Next matching mobility.turn_move_turn.forward report will be patched to UNEXPECTED_EXCEPTION.")
print("Expected: NMS should enter s7stopped / UNSAFE_STOP immediately, without location retry.")
