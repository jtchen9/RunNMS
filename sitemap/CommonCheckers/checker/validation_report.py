from __future__ import annotations
import hashlib, json
from pathlib import Path
from typing import Any, Dict, List

def sha256_file(path:str|Path)->str:
    h=hashlib.sha256(); path=Path(path)
    with path.open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024), b''): h.update(chunk)
    return 'sha256:'+h.hexdigest()

def make_report(*, ok:bool, issues:List[Dict[str,Any]], script_path, initial_poses_path, site_id:str, site_version:str, common_checkers_version:str):
    errors=[x for x in issues if x.get('level')=='error']; warnings=[x for x in issues if x.get('level')=='warning']
    return {"ok":bool(ok),"status":"pass" if ok else "fail","site_id":site_id,"site_version":site_version,"common_checkers_version":common_checkers_version,"script_path":str(script_path),"initial_poses_path":str(initial_poses_path),"script_hash":sha256_file(script_path),"initial_poses_hash":sha256_file(initial_poses_path) if Path(initial_poses_path).exists() else "","error_count":len(errors),"warning_count":len(warnings),"issues":issues}

def write_report(report:Dict[str,Any], path):
    Path(path).write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
