from t9_mobility_report_intercept import DEFAULT_SCANNER, arm_rule, rule_drop_report

scanner = DEFAULT_SCANNER
info = arm_rule(scanner, rule_drop_report())

print("A3 drop forward report armed")
print("scanner =", info["scanner"])
print("enable_key =", info["enable_key"])
print("rule_key =", info["rule_key"])
print("Next matching forward report will be dropped before Redis report storage.")
print("Expected: S1 should wait until report timeout.")