from t9_mobility_report_intercept import (
    DEFAULT_SCANNER,
    arm_rule,
)

scanner = DEFAULT_SCANNER

rule = {
    "mode": "patch",
    "match_action": "mobility.turn_move_turn.forward",
    "once": False,
    "patch": {
        "last_exec_status": "failed",
        "last_error_code": "MOVE_EXEC_FAIL",
        "last_error_detail": "debug injected repeated MOVE_EXEC_FAIL",
        "last_location_result": {
            "ok": False,
            "error": "debug injected repeated MOVE_EXEC_FAIL",
        },
    },
}

info = arm_rule(scanner, rule)

print("F2 repeated MOVE_EXEC_FAIL stop armed")
print("scanner =", info["scanner"])
print("enable_key =", info["enable_key"])
print("rule_key =", info["rule_key"])
print("Every matching mobility.turn_move_turn.forward report will be patched to MOVE_EXEC_FAIL.")
print("Expected: first MOVE_EXEC_FAIL enters location recovery; second MOVE_EXEC_FAIL hits unexpected-event sum limit and stops.")
