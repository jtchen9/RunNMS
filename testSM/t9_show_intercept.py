import json

from t9_mobility_report_intercept import (
    DEFAULT_SCANNER,
    enable_key,
    get_enable_value,
    get_intercept_rule,
    intercept_key,
    read_recent_intercept_events,
)

scanner = DEFAULT_SCANNER

print("scanner =", scanner)
print("enable_key =", enable_key(scanner))
print("enable_value =", get_enable_value(scanner))
print("rule_key =", intercept_key(scanner))
print()
print("current rule:")
print(json.dumps(get_intercept_rule(scanner), ensure_ascii=False, indent=2))
print()
print("recent events:")
for ev in read_recent_intercept_events(20):
    print(json.dumps(ev, ensure_ascii=False, indent=2))
    