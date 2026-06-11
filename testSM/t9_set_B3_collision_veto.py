from t9_mobility_report_intercept import DEFAULT_SCANNER, arm_rule, rule_collision_veto

scanner = DEFAULT_SCANNER
info = arm_rule(scanner, rule_collision_veto())

print("B3 collision veto armed")
print("scanner =", info["scanner"])
print("enable_key =", info["enable_key"])
print("rule_key =", info["rule_key"])
print("Next matching forward report will be patched to COLLISION_VETO.")
print("Expected: NMS should not advance planned pose as normal movement.")