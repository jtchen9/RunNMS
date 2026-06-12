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
        "last_error_code": "DEBUG_UNKNOWN_ERROR_CODE",
        "last_error_detail": "debug injected unknown error code for conservative-stop test",
        "last_location_result": {
            "ok": False,
            "error": "debug injected unknown error code for conservative-stop test",
        },
    },
}

info = arm_rule(scanner, rule)

print("K1 DEBUG_UNKNOWN_ERROR_CODE armed")
print("scanner =", info["scanner"])
print("enable_key =", info["enable_key"])
print("rule_key =", info["rule_key"])
print("Next matching mobility.turn_move_turn.forward report will be patched to DEBUG_UNKNOWN_ERROR_CODE.")
print("Expected: NMS should conservatively enter s7stopped / UNSAFE_STOP, without location retry.")
