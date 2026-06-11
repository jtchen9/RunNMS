from t9_mobility_report_intercept import (
    DEFAULT_SCANNER,
    arm_rule,
    rule_collision_stop_during_move,
)

scanner = DEFAULT_SCANNER
info = arm_rule(scanner, rule_collision_stop_during_move())

print("B3b collision stop-during-move armed")
print("scanner =", info["scanner"])
print("enable_key =", info["enable_key"])
print("rule_key =", info["rule_key"])
print("Next matching forward report will be patched to COLLISION_STOP_DURING_MOVE.")
print("Expected: NMS should enter collision/safety handling, not unknown-error handling.")
