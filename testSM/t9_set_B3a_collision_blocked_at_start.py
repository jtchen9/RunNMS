from t9_mobility_report_intercept import (
    DEFAULT_SCANNER,
    arm_rule,
    rule_collision_blocked_at_start,
)

scanner = DEFAULT_SCANNER
info = arm_rule(scanner, rule_collision_blocked_at_start())

print("B3a collision blocked-at-start armed")
print("scanner =", info["scanner"])
print("enable_key =", info["enable_key"])
print("rule_key =", info["rule_key"])
print("Next matching forward report will be patched to COLLISION_BLOCKED_AT_START.")
print("Expected: NMS should enter collision/safety handling, not unknown-error handling.")