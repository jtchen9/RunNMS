"""Offline NMS tool: select traces from the current setup and use Run Python File.
Analysis and calibration calculate identical proposals. Only calibration offers
one confirmed write. Use fresh observations after applying changes.
Distance uses the median actual/requested ratio across flat and bump moves.
Angular samples require robot evidence of executed legs. Unknown, recovered
and mixed-direction legs never fit stop margins. A composite pre-turn is usable
for alignment if final tag heading observes travel heading (no post-turn).
"""
from datetime import datetime
import json
import math
import os
import statistics
import tempfile
from collections import defaultdict
from pathlib import Path

MED = statistics.median
ACTION = 'mobility.turn_move_turn.forward'



def pose(p):
    return tuple(p.get(k) for k in ('x_m', 'y_m', 'heading_deg'))


def observations(paths, scanner, train, max_move_yaw=2.0):
    events = []
    seen = set()
    for path in paths:
        with path.open(encoding='utf-8-sig') as f:
            for line in f:
                r = json.loads(line)
                if r['scanner'] != scanner:
                    continue
                if r['trace_version'] != 'mobility_trace_v1':
                    raise ValueError('unsupported trace version')
                t = r['logged_at']  # fixed-width local trace clock; event ordering only
                key = (r['trace_id'], r['event'], t)
                if key not in seen:
                    events.append((t, r))
                    seen.add(key)
    previous = None
    by_id = defaultdict(dict)
    samples = []
    used = set()
    for t, r in sorted(events, key=lambda tr: tr[0]):
        e, d, i = r['event'], r['data'], r['trace_id']
        h = by_id[i]
        if e == 's5_command_computed':
            h['start_ok'] = previous is not None and pose(previous) == pose(d['start_pose'])
            h['computed'] = d
        elif e == 's3_pose_propagated':
            previous = None
        elif e == 's3_pose_solved':
            previous = d if (d.get('location_ok') and d.get('confidence_level') == 'HIGH'
                             and d.get('source') == 'apriltag_componentwise') else None
            h['end'] = previous
        elif e in ('s6_command_issued', 'mobility_report_received'):
            h[e] = d
        elif e == 's5_followup_decision' and i not in used:
            a = d['command_snapshot']
            g = a['args']
            issued = h.get('s6_command_issued', {})
            ack = h.get('mobility_report_received', {})
            executed = issued.get('args') or {}
            action = issued.get('action')
            if not (h.get('start_ok') and h.get('end')
                    and a == h.get('computed') and a['action'] == action == ack.get('last_command')
                    and action in (ACTION, 'mobility.turn')
                    and executed == ack.get('last_command_args')
                    and ack.get('last_exec_status') == 'completed' and not ack.get('last_error_code')
                    and pose(h['end']) == pose(d['solved_true_pose'])):
                continue
            ev = h['end'].get('movement_evidence') or {}
            before = a['start_pose']['heading_deg']
            after = h['end']['heading_deg']
            first = a.get('initial_script_execution')
            if action == 'mobility.turn':
                if ev.get('pre_kind') == 'direct':
                    pre = ev['pre_deg']
                    samples.append(('ccw' if pre > 0 else 'cw',
                                    math.copysign(1, pre)*wrap(after-before-pre)))
                continue
            if not first or ev.get('skipped'):
                continue
            requested = executed['distance_m']
            profile = executed.get('move_profile', 'default')
            if profile not in ('default','bump_crossing_up','bump_crossing_down'):
                continue
            if ev:
                length = ev['distance_m']
            elif profile != 'default':
                # Legacy robot overrode the issued distance with its local profile.
                direction = 'positive_y' if profile.endswith('_up') else 'negative_y'
                length = train['bump_crossing'][direction]['distance_m']
                if not math.isclose(length, requested):
                    continue
            else:
                length = requested
            if not math.isfinite(length) or length <= 0 or length < train['forward']['minimum_distance_m']:
                continue
            c = d['comparison']
            ratio = c['actual_displacement_m'] / length
            if not math.isfinite(ratio):
                continue
            samples.append(('distance:'+profile, ratio))
            along = c.get('along_track_displacement_m')
            cross = c.get('cross_track_displacement_m')
            if along is not None and cross is not None and math.hypot(along, cross) > 0:
                samples.append(('travel_error_deg', math.degrees(math.atan2(cross, along))))
            used.add(i)
            if not ev or ev.get('max_move_yaw_deg') is None or not 0 <= ev['max_move_yaw_deg'] <= max_move_yaw:
                continue  # old traces have no trustworthy composite/retry evidence
            pre, post = ev['pre_deg'], ev['post_deg']
            pk, qk = ev['pre_kind'], ev['post_kind']
            if qk == 'none' and pk in ('none','direct','composite') and c['actual_displacement_m'] > 0:
                start, end = a['start_pose'], h['end']
                bearing = math.degrees(math.atan2(end['y_m']-start['y_m'], end['x_m']-start['x_m']))
                samples.append(('alignment', wrap(bearing-after)))
            elif pk == 'direct' and qk in ('direct','composite','none') and c['actual_displacement_m'] > 0:
                start, end = a['start_pose'], h['end']
                bearing = math.degrees(math.atan2(end['y_m']-start['y_m'], end['x_m']-start['x_m']))
                samples.append((f"pair:{int(abs(pre)//30)}:{'ccw' if pre>0 else 'cw'}",
                                wrap(bearing-before-pre)))
            legs = [angle for angle,kind in ((pre,pk),(post,qk)) if kind == 'direct']
            if (legs and pk in ('none','direct') and qk in ('none','direct')
                    and all(x*legs[0] > 0 for x in legs)):
                # Equal-direction stop bias accumulates once per physical stop.
                residual = wrap(after-before-sum(legs))
                samples.append(('ccw' if legs[0] > 0 else 'cw',
                                math.copysign(1, legs[0])*residual/len(legs)))
    return samples


def wrap(deg):
    return (deg + 180) % 360 - 180


def run(path, traces, robot, mode, policy, trend_file):
    if mode not in ('analysis', 'calibration'):
        raise ValueError('MODE must be analysis or calibration')
    if min(policy['min_moves'], policy['min_angles']) < 1 or not 0 <= policy['distance_tolerance'] < 1 or policy['angle_tolerance_deg'] < 0:
        raise ValueError('invalid policy')
    raw = path.read_bytes()
    doc = json.loads(raw)
    entry = doc['robots'][robot]
    train = entry['drivetrain']
    if doc.get('schema_version') != 1 or train['model'] != 'balanced_timed_v2' or entry.get('production_loader_enabled') is not True:
        raise ValueError('expected enabled v2 calibration')
    groups = defaultdict(list)
    for key, value in observations(traces, robot, train, policy.get('max_move_yaw_deg', 2.0)):
        groups[key].append(value)
    distances = [v for k, seq in groups.items() if k.startswith('distance:') for v in seq]
    print(robot, {k: len(v) for k,v in groups.items()})
    changes = []
    metrics = [f"distance_ratio={MED(distances):.4f} n={len(distances)}" if distances else "distance_ratio=NA n=0"]
    travel = groups['travel_error_deg']
    if travel:
        metrics.append(f"travel_error_deg median={MED(travel):+.3f} median_abs={MED(map(abs, travel)):.3f} n={len(travel)}")
    if len(distances) >= policy['min_moves']:
        ratio = MED(distances)
        print(f'Distance: n={len(distances)}, median actual/requested={ratio:.4f}')
        if ratio > 0 and abs(1-ratio) > policy['distance_tolerance']:
            for label, profile in [('flat',train['forward']), *train['bump_crossing'].items()]:
                key = 'seconds_per_meter'
                changes.append((label, profile, key, profile[key]/ratio))
    else:
        print('Distance held: insufficient moves.')
    # Prefer camera-observed heading. Otherwise use equal sign weights within
    # comparable pre-turn magnitudes (assumes roughly symmetric turn bias).
    if len(groups['alignment']) < policy['min_angles']:
        estimates = []
        for k in list(groups):
            if k.startswith('pair:') and k.endswith(':cw'):
                cw, ccw = groups[k], groups[k[:-2]+'ccw']
                if min(len(cw),len(ccw)) >= policy['min_angles']:
                    estimates.append((MED(cw)+MED(ccw))/2)
        paired_offset = MED(estimates) if estimates else None
    else:
        paired_offset = None
    for group, obj, key in [('alignment',train,'travel_heading_offset_deg'),
                            ('ccw',train['turning'],'ccw_stop_margin_deg'),
                            ('cw',train['turning'],'cw_stop_margin_deg')]:
        old = obj.get(key, 0.0)
        values = groups[group]
        paired = group == 'alignment' and paired_offset is not None
        if not paired and len(values) < policy['min_angles']:
            metrics.append(f"{key}={old:g} insufficient n={len(values)}")
            print(f'{key}: hold {old:g} deg; {len(values)} eligible observations.')
            continue
        # bearing minus observed camera heading measures the absolute offset.
        # Convert to a residual so an already-applied offset is not counted twice.
        if paired:
            delta = wrap(paired_offset-old)
            print(f'Alignment estimated from {len(estimates)} matched CW/CCW pre-turn bins.')
        else:
            delta = MED([wrap(v-old) for v in values]) if group == 'alignment' else MED(values)
        evidence = f"matched_bins={len(estimates)} direct_n={len(values)}" if paired else f"n={len(values)}"
        metrics.append(f"{key} baseline={old:g} residual={delta:+.3f}deg {evidence}")
        candidate = wrap(old+delta) if group == 'alignment' else old+delta
        print(f'{key}: n={len(values)}, residual={delta:+.3f} deg')
        if group != 'alignment' and abs(candidate) > 30:
            print('Hold: outside the existing production loader domain (+/-30 deg).')
        elif abs(delta) > policy['angle_tolerance_deg']:
            changes.append((group, obj, key, candidate))
    for label, obj, key, new in changes:
        print(f'{label}.{key}: {obj.get(key,0):.6f} -> {new:.6f}')
    if mode == 'analysis':
        print('No file write.')
        return
    if changes and input(f'Type {robot} to apply all listed changes: ') != robot:
        print('Cancelled.')
        return
    details = [f"{label}.{key}: {obj.get(key,0):.6f} -> {new:.6f} (delta {new-obj.get(key,0):+.6f})"
               for label, obj, key, new in changes]
    trend_file = Path(trend_file)
    if trend_file.resolve() in {p.resolve() for p in [path, *traces]}:
        raise ValueError("Trend file must be separate from calibration and traces")
    trend_file.parent.mkdir(parents=True, exist_ok=True)
    # Open before changing the registry; mark applied only after replacement succeeds.
    with trend_file.open('a', encoding='utf-8') as log:
        if changes:
            for _, obj, key, new in changes:
                obj[key] = new
            with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as f:
                tmp = Path(f.name)
            try:
                tmp.write_text(json.dumps(doc, indent=2, sort_keys=True)+'\n', encoding='utf-8')
                tmp.chmod(path.stat().st_mode & 0o777)
                if path.read_bytes() != raw:
                    raise ValueError('registry changed since review; rerun')
                os.replace(tmp,path)
            finally:
                tmp.unlink(missing_ok=True)
            print(f'Updated {path}; review the Git diff before committing.')
        line = ' | '.join([datetime.now().strftime('%Y-%m-%d-%H:%M:%S'), robot,
                          'APPLIED' if changes else 'HOLD',
                          'trace=' + ', '.join(t.name for t in traces), *metrics, *details])
        try:
            log.write(line + '\n')
            log.flush()
            os.fsync(log.fileno())
        except OSError as exc:
            raise RuntimeError(f"Trend append failed; do not repeat calibration. Record manually: {line}") from exc
    print(f'Trend appended: {trend_file}')



if __name__ == '__main__':
    # VS Code: edit these settings, then Run Python File.
    CALIBRATION_FILE = Path(r"D:\Data\_Action\_RunScanner\robot_mobility_calibration.json")
    TREND_FILE = Path(r"D:\Data\_Action\_RunNMS\mobilityTrace\calibration_trend.txt")
    HISTORY_FILES = [Path(r"D:\Data\_Action\_RunNMS\mobilityTrace\output\mobility_trace.jsonl")]
    ROBOT = "twin-scout-delta"
    # MODE = "analysis"  # analysis or calibration: both calculate every proposal
    MODE = "calibration"  # analysis or calibration: both calculate every proposal
    POLICY = dict(min_moves=16, distance_tolerance=0.03,
                  min_angles=5, angle_tolerance_deg=1.0, max_move_yaw_deg=2.0)
    run(CALIBRATION_FILE, HISTORY_FILES, ROBOT, MODE, POLICY, TREND_FILE)
