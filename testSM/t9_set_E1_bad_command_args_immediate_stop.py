from t9_mobility_report_intercept import (
    DEFAULT_SCANNER,
    arm_rule,
)

scanner = DEFAULT_SCANNER

rule = {
    "mode": "patch",
    "match_action": "mobility.turn_move_turn.forward",
    "once": True,
    "patch": {
        "last_exec_status": "failed",
        "last_error_code": "BAD_COMMAND_ARGS",
        "last_error_detail": "debug injected BAD_COMMAND_ARGS immediate stop",
        "last_location_result": {
            "ok": False,
            "error": "debug injected BAD_COMMAND_ARGS",
        },
    },
}

info = arm_rule(scanner, rule)

print("E1 BAD_COMMAND_ARGS immediate-stop armed")
print("scanner =", info["scanner"])
print("enable_key =", info["enable_key"])
print("rule_key =", info["rule_key"])
print("Next matching mobility.turn_move_turn.forward report will be patched to BAD_COMMAND_ARGS.")
print("Expected: NMS should enter s7stopped / UNSAFE_STOP immediately, without location retry.")
