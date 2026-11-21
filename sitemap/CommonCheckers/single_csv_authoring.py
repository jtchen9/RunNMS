"""Pure authoring helpers. No NMS config, Redis, network or robot commands."""
from __future__ import annotations
import csv
import io
import json
import math
from pathlib import Path
from checker.registration_csv import parse_registration_csv, CsvFormatError, _read, CSV_COLUMNS

MOVING={'mobility.move','mobility.in2out','mobility.out2in'}

def _write(rows):
    out=io.StringIO(newline='');csv.writer(out,lineterminator='\n').writerows(rows)
    return out.getvalue().encode('utf-8-sig')

def _target(command,macro_policy):
    args=command['args'];x=args.get('x_m',args.get('target_x_m'));y=args.get('y_m',args.get('target_y_m'))
    for cfg in macro_policy.get('macros',{}).values():
        token=str(cfg.get('launch_constant','')).upper()
        if token and isinstance(x,str) and isinstance(y,str) and x.strip().upper()==y.strip().upper()==token:
            return float(cfg['start_x_m']),float(cfg['start_y_m'])
    try:
        if isinstance(x,bool) or isinstance(y,bool):raise ValueError()
        xy=float(x),float(y)
        if not all(math.isfinite(v) for v in xy):raise ValueError()
        return xy
    except (ValueError,TypeError,OverflowError):
        raise CsvFormatError('REENTRY_TARGET_REQUIRED','Re-entry mobility.move needs an absolute numeric target or a configured launch constant.',command['row_number'])

def expected_reminders(manifest,macro_policy):
    result={};by_robot={}
    for c in sorted(manifest['commands'],key=lambda c:(c['t_offset_sec'],c['row_number'])):
        if c['category'].lower() not in {'ap','ap_meta'}:by_robot.setdefault(c['scanner'],[]).append(c)
    for robot,commands in by_robot.items():
        for i,command in enumerate(commands[:-1]):
            if command['action']!='system.shutdown':continue
            first=commands[i+1];xy=None
            for later in commands[i+1:]:
                if later['action']=='system.shutdown':break
                if later['action'] not in MOVING:continue
                if later['action']!='mobility.move':
                    raise CsvFormatError('REENTRY_REQUIRES_STAGING_MOVE','First moving command after shutdown must be mobility.move before a macro.',later['row_number'])
                xy=_target(later,macro_policy);break
            result[first['row_number']]={'scanner':robot,'xy':xy}
    return result

def prepare_export(raw:bytes,macro_policy:dict):
    """Regenerate F/G and H without editing/reordering any A-E command fields."""
    rows=_read(raw)
    # The raw workbook draft already has the v2 header and its I/J roster.
    for row in rows[1:]:
        if len(row)>10:raise CsvFormatError('EXTRA_COLUMNS','Only columns A-J are supported.')
        row.extend(['']*(10-len(row)));row[5:7]=['','']
    manifest=parse_registration_csv(_write(rows))
    reminders=expected_reminders(manifest,macro_policy)
    command_rows={c['row_number'] for c in manifest['commands']}
    for n,row in enumerate(rows[1:],2):
        row[7]=f'=$H$1+B{n}/86400' if n in command_rows else ''
        if n in reminders:
            item=reminders[n];row[5]=item['scanner']
            if item['xy'] is not None:row[6]='('+', '.join(format(v,'.15g') for v in item['xy'])+')'
    result=_write(rows)
    parse_registration_csv(result)
    return result

def reminder_issues(manifest,macro_policy):
    """Authoring diagnostics only. Reminder coordinates never enter runtime args."""
    try:expected=expected_reminders(manifest,macro_policy)
    except CsvFormatError as exc:
        return [dict(level='error',code=exc.code,row_number=exc.row_number,message=str(exc))]
    actual={r['row_number']:r for r in manifest['reentry_reminders']};issues=[]
    for row in sorted(expected.keys()|actual.keys()):
        e=expected.get(row);a=actual.get(row)
        match=e is not None and a is not None and e['scanner']==a['scanner']
        if match:
            xy=e['xy']
            match=(a['x_m'] is None and a['y_m'] is None) if xy is None else (a['x_m'] is not None and a['y_m'] is not None and abs(a['x_m']-xy[0])<1e-8 and abs(a['y_m']-xy[1])<1e-8)
        if not match:issues.append(dict(level='error',code='REENTRY_REMINDER_MISMATCH',row_number=row,message='Regenerate F/G: each re-entry reminder belongs to the first command after shutdown and names its first move destination (or blank G for no move).'))
    return issues

def planned_poses_bytes(manifest):
    """Internal adapter for the existing route renderer, never a user upload."""
    rows=[['scanner','intended_x_m','intended_y_m','intended_heading_deg']]
    rows += [[p['scanner'],p['x_m'],p['y_m'],0] for p in manifest['initial_positions']]
    return _write(rows)

def checker_manifest(raw):
    m=parse_registration_csv(raw)
    # Absolute moves and crossing macros do not use an operator's initial heading.
    # Placeholder is for the existing static geometry model, not a measured pose.
    for p in m['initial_positions']:p['heading_deg']=0.
    return m
