from __future__ import annotations
from typing import Any, Dict, List, Set
from .script_model import InitialPose, ScriptRow

def mobility_scanners(rows:List[ScriptRow])->Set[str]:
    return {r.scanner for r in rows if r.category=='mobility'}

def check_first_mobility_command(rows:List[ScriptRow], policy:Dict[str,Any]):
    issues=[]; required=str(policy.get('first_mobility_action_required','mobility.report.location')); first={}
    for row in rows:
        if row.category!='mobility': continue
        old=first.get(row.scanner)
        if old is None or row.t_offset_sec<old.t_offset_sec: first[row.scanner]=row
    for scanner,row in sorted(first.items()):
        if row.action!=required:
            issues.append({"level":"error","code":"BAD_FIRST_MOBILITY_COMMAND","row_number":row.row_number,"scanner":scanner,"action":row.action,"message":f"first mobility command for {scanner} must be {required}, but row {row.row_number} uses {row.action}.","suggestion":f"Insert {required} as the earliest mobility row for this robot."})
    return issues

def check_initial_poses_exist(rows:List[ScriptRow], initial_poses:Dict[str,InitialPose]):
    return [{"level":"error","code":"MISSING_INITIAL_POSE","row_number":0,"scanner":s,"message":f"missing intended initial pose for mobility robot {s}.","suggestion":"Add this robot to initial_poses.csv."} for s in sorted(mobility_scanners(rows)) if s not in initial_poses]
