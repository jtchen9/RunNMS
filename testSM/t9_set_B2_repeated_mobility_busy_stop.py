from t9_mobility_report_intercept import DEFAULT_SCANNER, arm_rule

scanner = DEFAULT_SCANNER

rule = {
    "mode": "patch",
    "match_action": "mobility.turn_move_turn.forward",
    "once": False,
    "patch": {
        "last_exec_status": "failed",
        "last_error_code": "MOBILITY_BUSY",
        "last_error_detail": "debug injected repeated MOBILITY_BUSY",
        "last_location_result": {
            "ok": False,
            "error": "debug injected repeated MOBILITY_BUSY",
        },
    },
}

info = arm_rule(scanner, rule)

print("B2 repeated MOBILITY_BUSY stop armed")
print("scanner =", info["scanner"])
print("enable_key =", info["enable_key"])
print("rule_key =", info["rule_key"])
print("Every matching mobility.turn_move_turn.forward report will be patched to MOBILITY_BUSY.")
print("Expected: first MOBILITY_BUSY starts retry timer; second MOBILITY_BUSY hits unexpected-event sum limit and stops.")
