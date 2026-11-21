"""Run after importing RoundTrip_AlphaBravo.csv and clicking RunCommonCheckers.
No arguments required. Compares the generated CSV with the supplied example.
"""
import json
import sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'sitemap/CommonCheckers'))
from checker.registration_csv import parse_registration_csv
from single_csv_authoring import reminder_issues

EXPECTED=Path(__file__).parent/'step2_fixtures/RoundTrip_AlphaBravo.csv'
ACTUAL=ROOT/'sitemap/DemoRoom/script_authoring/generated/experiment_script.csv'
POLICY=ROOT/'sitemap/DemoRoom/script_authoring/config/macro_policy.json'

def compare(expected,actual,policy):
    a=parse_registration_csv(expected);b=parse_registration_csv(actual)
    def commands(m):return [(c['scanner'],c['t_offset_sec'],c['category'],c['action'],c['args']) for c in m['commands']]
    def roster(m):return {p['scanner']:(p['x_m'],p['y_m']) for p in m['initial_positions']}
    if commands(a)!=commands(b):raise ValueError('Command target, order, timing, action or args changed during the Excel round trip.')
    if roster(a)!=roster(b):raise ValueError('I/J roster or coordinates changed during the Excel round trip.')
    issues=reminder_issues(b,policy)
    if issues:raise ValueError(str(issues))
    for c in b['commands']:
        if c['display_time_cell']!=f"=$H$1+B{c['row_number']}/86400":raise ValueError('Wrong H formula at row '+str(c['row_number']))
    return f"PASS: {b['command_count']} commands, {b['robot_count']} initial robots, {len(b['reentry_reminders'])} re-entry reminder; command meaning, roster and H formulas retained."

if __name__=='__main__':
    try:
        if not ACTUAL.exists():raise ValueError('First import the supplied RoundTrip_AlphaBravo.csv into Excel, apply it, then click RunCommonCheckers. Missing: '+str(ACTUAL))
        print(compare(EXPECTED.read_bytes(),ACTUAL.read_bytes(),json.loads(POLICY.read_text(encoding='utf-8-sig'))))
    except (ValueError,OSError) as exc:
        print('FAIL: '+str(exc));raise SystemExit(1)
