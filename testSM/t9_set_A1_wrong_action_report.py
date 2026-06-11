from t9_mobility_report_intercept import DEFAULT_SCANNER, arm_rule, rule_wrong_action_report

scanner = DEFAULT_SCANNER
info = arm_rule(scanner, rule_wrong_action_report())

print("A1 wrong-action report armed")
print("scanner =", info["scanner"])
print("enable_key =", info["enable_key"])
print("rule_key =", info["rule_key"])
print("Next matching forward report will be replaced by mobility.report.location.")
print("Expected: S1 should ignore it, not accept forward as completed.")
