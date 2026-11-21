from __future__ import annotations
from typing import Any, Dict, List, Set
from .script_model import InitialPose, ScriptRow

def check_prepared_mobility_sequence(rows:List[ScriptRow], policy:Dict[str,Any]):
    """Preparation replaces the opening report, but not reports after re-entry.

    F/G are deliberately not consulted. Check authored command order only.
    Preserve the installed report-to-first-move spacing when a report exists.
    """
    issues=[]
    moving=set(policy.get('moving_mobility_actions', []))
    moving.update({'mobility.move','mobility.in2out','mobility.out2in'})
    min_gap=int(policy.get('min_seconds_after_report_location_before_first_move',60))
    by_robot={}
    for row in sorted(rows,key=lambda r:(r.t_offset_sec,r.row_number)):
        state=by_robot.setdefault(row.scanner,dict(reentry=False,first_move=True,report=None))
        if row.action=='system.shutdown':
            state.update(reentry=True,first_move=True,report=None)
            continue
        if row.category!='mobility':continue
        if row.action=='mobility.report.location' and state['first_move'] and state['report'] is None:
            state['report']=row
        if row.action not in moving or not state['first_move']:continue
        state['first_move']=False
        def error(code,message):
            issues.append(dict(level='error',code=code,row_number=row.row_number,
                               scanner=row.scanner,action=row.action,message=message))
        if state['reentry'] and row.action!='mobility.move':
            error('REENTRY_REQUIRES_STAGING_MOVE','First moving command after shutdown must be mobility.move; stage before a crossing macro.')
        report=state['report']
        if state['reentry'] and report is None:
            error('REENTRY_REQUIRES_LOCATION_REPORT','Add mobility.report.location after shutdown and before the first moving command.')
        if report is not None and row.t_offset_sec-report.t_offset_sec<min_gap:
            error('FIRST_MOVE_TOO_SOON_AFTER_REPORT_LOCATION',
                  f'Allow at least {min_gap}s after location report row {report.row_number} before this first move.')
    return issues

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
    first_missing_by_scanner: Dict[str, ScriptRow] = {}
    for row in rows:
        if row.category != "mobility" or row.scanner in initial_poses:
            continue
        old = first_missing_by_scanner.get(row.scanner)
        if old is None or row.t_offset_sec < old.t_offset_sec:
            first_missing_by_scanner[row.scanner] = row

    return [
        {
            "level": "error",
            "code": "MISSING_INITIAL_POSE",
            "row_number": row.row_number,
            "scanner": scanner,
            "category": row.category,
            "action": row.action,
            "message": (
                f"missing intended initial pose for mobility robot {scanner}."
            ),
            "suggestion": "Add this robot to initial_poses.csv.",
        }
        for scanner, row in sorted(first_missing_by_scanner.items())
    ]
