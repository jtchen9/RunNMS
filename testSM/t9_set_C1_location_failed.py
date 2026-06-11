from t9_mobility_report_intercept import DEFAULT_SCANNER, arm_rule, rule_location_failed

scanner = DEFAULT_SCANNER
info = arm_rule(scanner, rule_location_failed())

print("C1 location failed armed")
print("scanner =", info["scanner"])
print("enable_key =", info["enable_key"])
print("rule_key =", info["rule_key"])
print("Next matching mobility.report.location report will be patched to LOCATION_SOLVE_FAILED.")
print("Expected: NMS should not update true_location_json with invalid pose.")
