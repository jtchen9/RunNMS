"""Read-only AutoLab command reconstruction. Standard library; Python >=3.10."""
from __future__ import annotations
import csv,hashlib,json,math
from pathlib import Path
VERSION='0.5.0'
COMMAND_COLUMNS=['scanner','t_offset_sec','category','action','args_json']
POINT_COLUMNS=['scanner','event_id','source_line','source_line_end','action','t_offset_sec','from_x_m','from_y_m','x_m','y_m','dx_m','dy_m','length_m','segment_known','status']

def number(value):
    if isinstance(value,bool): raise ValueError('Boolean is not a coordinate/time')
    try: n=float(value)
    except OverflowError as exc: raise ValueError("Value exceeds numeric range") from exc
    if not math.isfinite(n): raise ValueError('Value must be finite')
    return n

def reconstruct(commands,initial_poses,policy=None, *, macro_policy=None):
    """Return JSON-serializable reconstruction; never write or repair input files."""
    diagnostics=[]; inputs=[]
    def issue(code,message,source,line=None,scanner=None,severity='error'):
        diagnostics.append(dict(severity=severity,code=code,message=message,source=str(source),source_line=line,scanner=scanner))
    def read_rows(path,required,*,command_sheet=False):
        path=Path(path)
        try:
            raw=path.read_bytes();inputs.append(dict(path=str(path),sha256=hashlib.sha256(raw).hexdigest(),size_bytes=len(raw)))
            text=raw.decode('utf-8-sig')
            import io
            reader=csv.reader(io.StringIO(text,newline=''),strict=True)
            header=next(reader,None)
            if not header or any(header.count(col) != 1 for col in required):
                issue('CSV_HEADER','Each required header must appear exactly once: '+','.join(required),path,1);return []
            if not command_sheet and len(set(header)) != len(header):
                issue('CSV_HEADER','Duplicate initial-pose header',path,1);return []
            # Excel F-H may contain blank headers and a local-time formula.
            fields = [(header.index(col), col) for col in required] if command_sheet else list(enumerate(header))
            result=[]
            while True:
                start=reader.line_num+1
                row=next(reader,None)
                if row is None:break
                if not row:continue
                if len(row)!=len(header):
                    issue('CSV_WIDTH','Row column count differs from header',path,start)
                entry={col:row[j] for j,col in fields if j<len(row)};entry['_line']=start;entry['_end']=reader.line_num;entry['_bad_width']=len(row)!=len(header)
                result.append(entry)
            return result
        except (OSError,UnicodeError,csv.Error) as exc:
            issue('CSV_READ',str(exc),path,locals().get('start'));return locals().get('result',[])
    vocab=None
    if policy:
        try:
            raw=Path(policy).read_bytes();inputs.append(dict(path=str(policy),sha256=hashlib.sha256(raw).hexdigest(),size_bytes=len(raw)))
            p=json.loads(raw.decode('utf-8-sig'));vocab=p.get('allowed_actions_by_category')
            if not isinstance(vocab,dict) or any(not isinstance(v,list) for v in vocab.values()):raise ValueError('allowed_actions_by_category must map categories to lists')
        except (OSError,ValueError,TypeError) as exc:issue('POLICY_READ',str(exc),policy);vocab=None
    starts={};duplicates=set()
    for r in read_rows(initial_poses,['scanner','intended_x_m','intended_y_m']):
        name=r.get('scanner','')
        if not name.strip():issue('INITIAL_SCANNER','Missing scanner',initial_poses,r['_line']);continue
        if name in starts:
            duplicates.add(name);issue('INITIAL_DUPLICATE','Duplicate initial pose; no entry selected',initial_poses,r['_line'],name)
        try:
            if r['_bad_width']:raise ValueError('Malformed CSV row')
            xy=(number(r['intended_x_m']),number(r['intended_y_m']))
            starts[name]=dict(x_m=xy[0],y_m=xy[1],source_line=r['_line'],kind='intended_initial_pose')
        except (ValueError,TypeError,KeyError) as exc:
            starts[name]=None;issue('INITIAL_COORDINATE',str(exc),initial_poses,r['_line'],name)
    for n in duplicates:starts[n]=None
    events=[];unorderable=set();unidentified_movement=False
    for idx,r in enumerate(read_rows(commands,COMMAND_COLUMNS,command_sheet=True),1):
        name=r.get('scanner','');category=r.get('category','');action=r.get('action','')
        moving=category=='mobility' or action.startswith('mobility.')
        e=dict(event_id=idx,source_line=r['_line'],source_line_end=r['_end'],scanner=name,category=category,action=action,raw={k:v for k,v in r.items() if not k.startswith('_')},t_offset_sec=None,args=None,valid=True,mobility=moving,status='event')
        def err(code,msg):
            e['valid']=False;issue(code,msg,commands,r['_line'],name or None)
        if r['_bad_width']:err('CSV_WIDTH_EVENT','Malformed row cannot be interpreted')
        if not name.strip():
            err('SCANNER_MISSING','Missing scanner')
            if moving:unidentified_movement=True
        try:
            t=number(r.get('t_offset_sec'))
            if t<0:raise ValueError('Time offset cannot be negative')
            e['t_offset_sec']=t
        except (ValueError,TypeError) as exc:
            err('TIME_INVALID',str(exc))
            if moving:unorderable.add(name)
        try:
            def pairs(items):
                d={}
                for k,v in items:
                    if k in d:raise ValueError('Duplicate JSON key: '+k)
                    d[k]=v
                return d
            args=json.loads(r.get('args_json',''),object_pairs_hook=pairs,parse_float=number,parse_constant=lambda s: (_ for _ in ()).throw(ValueError('Nonfinite JSON number: '+s)))
            if not isinstance(args,dict):raise ValueError('args_json must be an object')
            e['args']=args
        except (ValueError,TypeError) as exc:err('ARGS_JSON',str(exc))
        if vocab is not None and action not in vocab.get(category,[]):err('ACTION_UNKNOWN','Action/category not in supplied vocabulary')
        if action.startswith('mobility.') and category!='mobility':err('CATEGORY_MISMATCH','Mobility action has a non-mobility category')
        events.append(e)
    macro_configs = {}
    macro_error = None
    if any(e['action'] in ('mobility.in2out', 'mobility.out2in') for e in events):
        try:
            if macro_policy is None:
                raise ValueError('Set macro_policy to DemoRoom/script_authoring/config/macro_policy.json')
            raw = Path(macro_policy).read_bytes()
            inputs.append(dict(path=str(macro_policy), sha256=hashlib.sha256(raw).hexdigest(), size_bytes=len(raw)))
            macro_configs = json.loads(raw.decode('utf-8-sig'))['macros']
            if not isinstance(macro_configs, dict): raise ValueError('macros must be an object')
        except (OSError, ValueError, KeyError, TypeError) as exc:
            macro_error = str(exc)
    robots={}
    for name in sorted({e['scanner'] for e in events if e['mobility'] and e['scanner'].strip()}):
        ee=sorted([e for e in events if e['scanner']==name and e['mobility']],key=lambda e:(e['t_offset_sec'] is None,e['t_offset_sec'] or 0,e['source_line']))
        initial=starts.get(name);current=(initial['x_m'],initial['y_m']) if initial else None
        if not initial:issue('INITIAL_UNAVAILABLE','No unambiguous valid initial pose; first incoming segment unknown',initial_poses,None,name)
        blocked=name in unorderable or unidentified_movement
        if blocked:issue('ORDER_UNCERTAIN','Movement with unknown time or robot prevents trustworthy segment reconstruction',commands,None,name)
        groups={}
        for e in ee:
            if e['t_offset_sec'] is not None:groups.setdefault(e['t_offset_sec'],[]).append(e)
        conflicts={t for t,g in groups.items() if sum(e['action']!='mobility.report.location' for e in g)>1}
        points=[]
        for e in ee:
            e.update(from_x_m=None,from_y_m=None,x_m=None,y_m=None,dx_m=None,dy_m=None,length_m=None,segment_known=False)
            if e['t_offset_sec'] in conflicts:
                e['status']='ambiguous_time';current=None
                issue('SIMULTANEOUS_MOVEMENT','Multiple movement commands for this robot at the same issue time',commands,e['source_line'],name)
            elif not e['valid']:
                e['status']='invalid';current=None
            elif e['action']=='mobility.report.location':
                e['status']='location_report_request'
                # No measured position is present in this command; retain intended state only.
            elif e['action']=='mobility.move':
                try:
                    args=e['args']
                    if 'dx_m' in args or 'dy_m' in args:raise ValueError('Relative/mixed movement is unsupported; require absolute x_m/y_m')
                    if not isinstance(args['x_m'],(int,float)) or not isinstance(args['y_m'],(int,float)):raise ValueError('Coordinates must be JSON numbers')
                    target=(number(args['x_m']),number(args['y_m']))
                    e['x_m'],e['y_m']=target
                    if current is not None and not blocked:
                        e['from_x_m'],e['from_y_m']=current
                        e['dx_m']=target[0]-current[0];e['dy_m']=target[1]-current[1]
                        e['length_m']=math.hypot(e['dx_m'],e['dy_m']);e['segment_known']=True;e['status']='commanded_segment'
                    else:e['status']='destination_only'
                    current=target
                except (KeyError,ValueError,TypeError) as exc:
                    e['status']='invalid_coordinates';current=None;issue('MOVE_COORDINATES',str(exc),commands,e['source_line'],name)
            elif e['action'] in ('mobility.in2out', 'mobility.out2in'):
                try:
                    if macro_error: raise ValueError(macro_error)
                    cfg = macro_configs[e['action']]
                    if e['args']: raise ValueError('Crossing commands do not accept arguments')
                    if cfg.get('target_mode') != 'runtime_relative_true_pose':
                        raise ValueError('Unsupported macro target_mode')
                    if current is None or blocked: raise ValueError('Crossing requires a known preceding position')
                    distance = number(cfg['distance_m'])
                    heading = number(cfg['target_heading_deg'])
                    if distance <= 0: raise ValueError('Crossing distance must be positive')
                    angle = math.radians(heading)
                    dx, dy = distance * math.cos(angle), distance * math.sin(angle)
                    dx = 0.0 if abs(dx) < 1e-12 else dx
                    dy = 0.0 if abs(dy) < 1e-12 else dy
                    target = (current[0] + dx, current[1] + dy)
                    number(target[0]); number(target[1])
                    e.update(from_x_m=current[0], from_y_m=current[1], x_m=target[0], y_m=target[1],
                             dx_m=dx, dy_m=dy, length_m=distance, segment_known=True,
                             status='expected_macro_segment', geometry_basis='planned launch + macro distance/heading')
                    current = target
                except (ValueError, KeyError, TypeError) as exc:
                    e['status']='unresolved_movement'; current=None
                    issue('MACRO_GEOMETRY', str(exc), commands, e['source_line'], name)
            else:
                e['status']='unresolved_movement';current=None
                issue('MOVEMENT_UNRESOLVED','No geometry available for '+e['action'],commands,e['source_line'],name)
            if e['action']!='mobility.report.location':points.append(e)
        robots[name]=dict(initial_pose=initial,events=ee,points=points)
    errors=sum(d['severity']=='error' for d in diagnostics)
    return dict(schema_version=1,tool_version=VERSION,description='Commanded destinations and straight planned segments; no arrival-time or safety inference',status='errors' if errors else 'ok',route_complete=errors==0,inputs=inputs,events=events,robots=robots,diagnostics=diagnostics,summary=dict(command_rows=len(events),robots=len(robots),movement_rows=sum(len(r['points']) for r in robots.values()),known_segments=sum(e.get('segment_known',False) for e in events),errors=errors),safety_checked=False)

def write_outputs(result,output_dir):
    out=Path(output_dir)
    sources={Path(i['path']).resolve() for i in result['inputs']}
    if any((out/f).resolve() in sources for f in ['reconstruction.json','diagnostics.json','route_points.csv']):raise ValueError('Output would overwrite an input file')
    out.mkdir(parents=True,exist_ok=True)
    (out/'reconstruction.json').write_text(json.dumps(result,indent=2,allow_nan=False)+'\n',encoding='utf-8')
    (out/'diagnostics.json').write_text(json.dumps(result['diagnostics'],indent=2)+'\n',encoding='utf-8')
    with (out/'route_points.csv').open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=POINT_COLUMNS,extrasaction='ignore');w.writeheader()
        for r in result['robots'].values():
            for e in r['points']:w.writerow({k:e.get(k) for k in POINT_COLUMNS})

def run(commands, initial_poses, policy, output_dir, *, macro_policy=None, map_workbook=None,
        map_sheet="StaticMap", map_cell_range="C4:DL117", resolution_m=0.1,
        dpi=160, per_robot=True, overview_label_every=5):
    """Return reconstruction plus a stable integration_result; no import side effects.

    Build in a fresh staging directory. Only this call's published files appear in
    integration_result. Callers must not discover outputs with a directory glob.
    """
    import tempfile, uuid, os
    from datetime import datetime, timezone
    result = None
    out = Path(output_dir)
    envelope = dict(schema_version=1, run_id=uuid.uuid4().hex,
                    generated_at_utc=datetime.now(timezone.utc).isoformat(),
                    status='error', success=False, exit_code=2,
                    png_files=[], files=[], diagnostics=[], inputs=[],
                    safety_checked=False, route_kind='planned',
                    result_file=None)
    stage_name = 'reconstruction'
    try:
        result = reconstruct(commands, initial_poses, policy, macro_policy=macro_policy)
        envelope['inputs'] = list(result['inputs'])
        envelope['diagnostics'] = list(result['diagnostics'])
        out.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='route_preview_') as stage:
            write_outputs(result, stage)
            if map_workbook is not None:
                stage_name = 'rendering'
                if __package__:
                    from .route_render import render_routes
                else:
                    from route_render import render_routes
                manifest = render_routes(
                    result, map_workbook, stage, sheet=map_sheet,
                    cell_range=map_cell_range, resolution_m=resolution_m,
                    dpi=dpi, per_robot=per_robot,
                    overview_label_every=overview_label_every)
                result['render_manifest'] = manifest
                envelope['inputs'].append(dict(path=str(map_workbook),
                                                sha256=manifest['map']['sha256']))
                for message in manifest['render_diagnostics']:
                    envelope['diagnostics'].append(dict(severity='error', code='RENDER_DIAGNOSTIC',
                        message=message, source=str(map_workbook), source_line=None, scanner=None))
            stage_name = 'publication'
            files = sorted(Path(stage).iterdir())
            protected = {Path(x).resolve() for x in
                         [commands, initial_poses, policy, macro_policy, map_workbook] if x is not None}
            if any((out / name).resolve() in protected for name in
                   [f.name for f in files] + ['preview_result.json']):
                raise ValueError('Preview output would overwrite an input file')
            for f in files:
                # Atomic replacement per file, including across filesystem boundaries.
                with tempfile.NamedTemporaryFile(dir=out, prefix='.preview_', delete=False) as tmp:
                    temp_path = Path(tmp.name)
                    try:
                        tmp.write(f.read_bytes())
                    except Exception:
                        temp_path.unlink(missing_ok=True)
                        raise
                try:
                    os.replace(temp_path, out / f.name)
                finally:
                    temp_path.unlink(missing_ok=True)
                record = dict(name=f.name, path=str((out/f.name).resolve()),
                              sha256=hashlib.sha256(f.read_bytes()).hexdigest())
                envelope['files'].append(record)
                if f.suffix.lower() == '.png': envelope['png_files'].append(record)
        envelope['success'] = not any(d['severity']=='error' for d in envelope['diagnostics'])
        envelope['status'] = 'ok' if envelope['success'] else 'incomplete'
        envelope['exit_code'] = 0 if envelope['success'] else 2
        stage_name = 'result_manifest'
        envelope['result_file'] = str((out/'preview_result.json').resolve())
        with tempfile.NamedTemporaryFile(dir=out, prefix='.preview_', mode='w', encoding='utf-8', delete=False) as tmp:
            temp_path = Path(tmp.name)
            try:
                json.dump(envelope, tmp, indent=2, allow_nan=False)
                tmp.write('\n')
            except Exception:
                temp_path.unlink(missing_ok=True)
                raise
        try:
            os.replace(temp_path, out/'preview_result.json')
        finally:
            temp_path.unlink(missing_ok=True)
    except Exception as exc:
        # A failed publication may leave files, but none are advertised as a usable preview.
        envelope.update(status='error', success=False, exit_code=2,
                        png_files=[], files=[], result_file=None)
        envelope['diagnostics'].append(dict(severity='error', code='PREVIEW_FAILED',
            message=f'{stage_name}: {type(exc).__name__}: {exc}', source=str(out),
            source_line=None, scanner=None))
    if result is None:
        result = dict(status='errors', route_complete=False, summary={'errors':1}, diagnostics=[], robots={})
    result['integration_result'] = envelope
    return result


if __name__ == "__main__":
    # Edit the paths here, then click Run Python File in VS Code.
    # Resolve paths from this shared package, independent of the working directory.
    # Keep CommonCheckers and DemoRoom as siblings in the distributed sitemap folder.
    COMMONCHECKERS_DIR = Path(__file__).resolve().parent
    SITEMAP_DIR = COMMONCHECKERS_DIR.parent
    AUTHORING_DIR = SITEMAP_DIR / "DemoRoom" / "script_authoring"
    GENERATED_DIR = AUTHORING_DIR / "generated"

    # Suggested defaults match the XLSM template's generated filenames.
    # For a separately saved experiment, point these to that experiment's CSVs.
    COMMANDS_CSV = GENERATED_DIR / "experiment_script.csv"
    INITIAL_POSES_CSV = GENERATED_DIR / "initial_poses.csv"
    POLICY_JSON = COMMONCHECKERS_DIR / "config" / "script_policy.json"
    # Set POLICY_JSON = None to omit command-vocabulary checks.
    OUTPUT_DIR = GENERATED_DIR / "route_preview"

    MACRO_POLICY_JSON = AUTHORING_DIR / "config" / "macro_policy.json"
    MAP_WORKBOOK = AUTHORING_DIR / "AutoLab_DemoRoom_Template.xlsm"
    MAP_SHEET = "StaticMap"
    MAP_CELL_RANGE = "C4:DL117"
    RESOLUTION_M = 0.1
    PNG_DPI = 160
    PER_ROBOT_PNG = True
    OVERVIEW_LABEL_EVERY = 5

    result = run(
        commands=COMMANDS_CSV,
        initial_poses=INITIAL_POSES_CSV,
        policy=POLICY_JSON,
        output_dir=OUTPUT_DIR,
        macro_policy=MACRO_POLICY_JSON,
        map_workbook=MAP_WORKBOOK,
        map_sheet=MAP_SHEET,
        map_cell_range=MAP_CELL_RANGE,
        resolution_m=RESOLUTION_M,
        dpi=PNG_DPI,
        per_robot=PER_ROBOT_PNG,
        overview_label_every=OVERVIEW_LABEL_EVERY,
    )
    print(json.dumps(result['integration_result'], indent=2))
    raise SystemExit(result['integration_result']['exit_code'])
