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
        "last_exec_status": "completed",
        "last_error_code": "NO_TAG_VISIBLE",
        "last_error_detail": "debug injected no tag visible after movement",
        "last_location_result": {
            "ok": True,
            "error": "debug injected NO_TAG_VISIBLE",
            "apriltag": {
                "ok": False,
                "count": 0,
                "tags": [],
            },
        },
    },
}

info = arm_rule(scanner, rule)

print("D1 no-tag-visible on forward armed")
print("scanner =", info["scanner"])
print("enable_key =", info["enable_key"])
print("rule_key =", info["rule_key"])
print("Next matching mobility.turn_move_turn.forward report will be patched to completed / NO_TAG_VISIBLE.")
print("Expected: NMS should propagate true_location by the exact issued command vector, not issue location retry.")
