from __future__ import annotations
from typing import Any, Dict, List
from .script_model import ScriptRow

def check_global_moving_command_spacing(rows:List[ScriptRow], policy:Dict[str,Any]):
    issues=[]; moving=set(policy.get('moving_mobility_actions',[])); min_gap=int(policy.get('global_min_seconds_between_moving_mobility_commands',180))
    moving_rows=sorted([r for r in rows if r.category=='mobility' and r.action in moving], key=lambda r:(r.t_offset_sec,r.row_number))
    for prev,cur in zip(moving_rows,moving_rows[1:]):
        gap=int(cur.t_offset_sec)-int(prev.t_offset_sec)
        if gap<min_gap:
            issues.append({"level":"error","code":"MOVING_COMMANDS_TOO_CLOSE","row_number":cur.row_number,"scanner":cur.scanner,"action":cur.action,"message":f"moving mobility command at row {cur.row_number} starts {gap} seconds after row {prev.row_number}; minimum is {min_gap} seconds.","suggestion":f"Move row {cur.row_number} to at least t_offset_sec={prev.t_offset_sec+min_gap}, or remove one movement.","previous_row_number":prev.row_number,"previous_scanner":prev.scanner,"previous_action":prev.action,"minimum_gap_sec":min_gap,"actual_gap_sec":gap})
    return issues
