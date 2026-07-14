from __future__ import annotations
from typing import Any, Dict, List
from .script_model import ScriptRow

def check_vocabulary(rows:List[ScriptRow], policy:Dict[str,Any]):
    issues=[]; allowed=set(policy.get('allowed_mobility_actions',[])); blocked=set(policy.get('blocked_mobility_actions',[]))
    for row in rows:
        if row.category!='mobility': continue
        if row.action in blocked:
            issues.append({"level":"error","code":"BLOCKED_MOBILITY_ACTION","row_number":row.row_number,"scanner":row.scanner,"action":row.action,"message":f"{row.action} is not allowed in AutoLab scripts.","suggestion":"Use semantic commands: mobility.report.location, mobility.move, mobility.in2out, or mobility.out2in."}); continue
        if row.action not in allowed:
            issues.append({"level":"error","code":"UNKNOWN_MOBILITY_ACTION","row_number":row.row_number,"scanner":row.scanner,"action":row.action,"message":f"unknown or unsupported mobility action: {row.action}","suggestion":f"Allowed mobility actions are: {sorted(allowed)}."})
    return issues
