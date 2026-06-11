from t9_mobility_report_intercept import (
    DEFAULT_SCANNER,
    clear_intercept,
    enable_key,
    intercept_key,
)

scanner = DEFAULT_SCANNER
n = clear_intercept(scanner, clear_events=True)

print("cleared mobility-report intercept and event stream")
print("scanner =", scanner)
print("enable_key =", enable_key(scanner))
print("rule_key =", intercept_key(scanner))
print("deleted =", n)
