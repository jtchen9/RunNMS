"""
F2 setup helper for deterministic exec_fail counter-limit test.

This version intentionally does NOT arm a mobility report intercept rule.
The F2 counter-limit test injects the matching failed report directly into
NMS state storage, then calls m8mobility.on_report_received().

Reason:
- The old F2 tried to naturally generate two MOVE_EXEC_FAIL reports through:
    MOVE_EXEC_FAIL -> location retry success -> second forward command
- But after a healthy location retry, the state machine can reset/recover and
  may not issue a second forward command. That made the old test assumption
  invalid and caused timeouts.
- This test is about the counter-limit branch itself, so we seed
  exec_fail_count = limit - 1 and inject the next MOVE_EXEC_FAIL.
"""

from t9_mobility_report_intercept import DEFAULT_SCANNER, clear_intercept

scanner = DEFAULT_SCANNER
clear_intercept(scanner, clear_events=True)

print("F2 deterministic MOVE_EXEC_FAIL counter-limit setup complete")
print("scanner =", scanner)
print("No intercept rule is armed for this test.")
print("Run: python .\\t9_F2_repeated_move_exec_fail_stop_v2.py")
