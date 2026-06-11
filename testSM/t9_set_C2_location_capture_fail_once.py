from t9_mobility_report_intercept import DEFAULT_SCANNER, arm_rule

scanner = DEFAULT_SCANNER

rule = {
    "mode": "patch",
    "match_action": "mobility.report.location",
    "once": True,
    "patch": {
        "last_exec_status": "failed",
        "last_error_code": "LOCATION_CAPTURE_FAIL",
        "last_error_detail": "debug injected location capture failure",
        "last_location_result": {
            "ok": False,
            "error": "debug injected LOCATION_CAPTURE_FAIL",
            "apriltag": {
                "ok": False,
                "count": 0,
                "tags": [],
            },
        },
    },
}

info = arm_rule(scanner, rule)

print("C2 location capture fail once armed")
print("scanner =", info["scanner"])
print("enable_key =", info["enable_key"])
print("rule_key =", info["rule_key"])
print("Next matching mobility.report.location report will be patched to LOCATION_CAPTURE_FAIL.")
print("Expected: NMS should issue another mobility.report.location and recover when it succeeds.")
