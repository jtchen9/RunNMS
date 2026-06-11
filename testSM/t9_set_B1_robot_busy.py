from t9_mobility_report_intercept import DEFAULT_SCANNER, arm_rule, rule_robot_busy

scanner = DEFAULT_SCANNER
info = arm_rule(scanner, rule_robot_busy())

print("B1 robot busy armed")
print("scanner =", info["scanner"])
print("enable_key =", info["enable_key"])
print("rule_key =", info["rule_key"])
print("Next matching forward report will be patched to MOBILITY_BUSY.")
print("Expected: NMS should not treat it as completed movement.")