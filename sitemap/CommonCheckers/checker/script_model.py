from __future__ import annotations
import csv, json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Tuple

@dataclass
class ScriptRow:
    row_number:int; scanner:str; t_offset_sec:int; category:str; action:str; args:Dict[str,Any]=field(default_factory=dict)

@dataclass
class InitialPose:
    row_number:int; scanner:str; x_m:float; y_m:float; heading_deg:float; position_tolerance_m:float; heading_tolerance_deg:float

def _cell(row:Dict[str,Any], key:str, default:str="")->str:
    return str(row.get(key, default) or default).strip()

def _parse_args_json(raw:str, row_number:int):
    raw=str(raw or "").strip()
    if not raw: return {}, []
    try: data=json.loads(raw)
    except Exception as e:
        return {}, [{"level":"error","code":"BAD_ARGS_JSON","row_number":row_number,"message":f"args_json is not valid JSON: {type(e).__name__}: {e}","suggestion":"Use an empty cell or a valid JSON object such as {}."}]
    if not isinstance(data, dict):
        return {}, [{"level":"error","code":"ARGS_JSON_NOT_OBJECT","row_number":row_number,"message":"args_json must decode to a JSON object.","suggestion":"Use an object such as {\"dx_m\": 1.0, \"dy_m\": 0.0}."}]
    return data, []

def load_script_csv(path:str|Path):
    path=Path(path); rows=[]; issues=[]
    with path.open('r',encoding='utf-8-sig',newline='') as f:
        reader=csv.DictReader(f); required={"scanner","t_offset_sec","category","action"}
        missing=sorted(required-set(reader.fieldnames or []))
        if missing: return [], [{"level":"error","code":"SCRIPT_CSV_MISSING_COLUMNS","row_number":0,"message":f"script CSV missing required columns: {missing}","suggestion":"Use columns: scanner,t_offset_sec,category,action,args_json."}]
        for i,raw in enumerate(reader,start=2):
            scanner=_cell(raw,'scanner'); category=_cell(raw,'category','scan').lower(); action=_cell(raw,'action')
            try: off=int(_cell(raw,'t_offset_sec','0'))
            except Exception:
                issues.append({"level":"error","code":"BAD_T_OFFSET_SEC","row_number":i,"message":"t_offset_sec must be an integer number of seconds.","suggestion":"Use values such as 0, 30, 180, 360."}); continue
            args,arg_issues=_parse_args_json(_cell(raw,'args_json'),i); issues.extend(arg_issues)
            if not scanner: issues.append({"level":"error","code":"MISSING_SCANNER","row_number":i,"message":"scanner is required.","suggestion":"Select a robot name."}); continue
            if not action: issues.append({"level":"error","code":"MISSING_ACTION","row_number":i,"message":"action is required.","suggestion":"Choose an allowed command."}); continue
            rows.append(ScriptRow(i,scanner,off,category,action,args))
    return rows, issues

def load_initial_poses_csv(path:str|Path):
    path=Path(path); poses={}; issues=[]
    if not path.exists(): return poses,[{"level":"error","code":"INITIAL_POSES_CSV_MISSING","row_number":0,"message":f"initial poses file not found: {path}","suggestion":"Provide initial_poses.csv."}]
    with path.open('r',encoding='utf-8-sig',newline='') as f:
        reader=csv.DictReader(f); required={"scanner","intended_x_m","intended_y_m","intended_heading_deg"}
        missing=sorted(required-set(reader.fieldnames or []))
        if missing: return poses,[{"level":"error","code":"INITIAL_POSES_CSV_MISSING_COLUMNS","row_number":0,"message":f"initial poses CSV missing required columns: {missing}","suggestion":"Use scanner,intended_x_m,intended_y_m,intended_heading_deg,position_tolerance_m,heading_tolerance_deg."}]
        for i,raw in enumerate(reader,start=2):
            scanner=_cell(raw,'scanner')
            if not scanner: issues.append({"level":"error","code":"INITIAL_POSE_MISSING_SCANNER","row_number":i,"message":"scanner is required in initial_poses.csv.","suggestion":"Add a robot name."}); continue
            try:
                pose=InitialPose(i,scanner,float(_cell(raw,'intended_x_m')),float(_cell(raw,'intended_y_m')),float(_cell(raw,'intended_heading_deg')),float(_cell(raw,'position_tolerance_m','0.20')),float(_cell(raw,'heading_tolerance_deg','10.0')))
            except Exception as e:
                issues.append({"level":"error","code":"BAD_INITIAL_POSE_VALUE","row_number":i,"message":f"initial pose has a bad numeric value: {type(e).__name__}: {e}","suggestion":"Use numeric meters/degrees."}); continue
            if scanner in poses: issues.append({"level":"error","code":"DUPLICATE_INITIAL_POSE","row_number":i,"message":f"duplicate initial pose for scanner {scanner}.","suggestion":"Keep exactly one initial pose row per robot."}); continue
            poses[scanner]=pose
    return poses, issues
