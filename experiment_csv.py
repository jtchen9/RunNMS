"""Read-only experiment CSV parsing. No Redis, commands, timers or robot imports.

F/G initial roster: contiguous entries from row 2, terminated by a blank pair.
Subsequent F/G pairs are reminders only. H is retained but never evaluated.
This is format preview, NOT approval to execute a script.
"""
from __future__ import annotations

import csv
import io
import json
import math
import re
from typing import Any

COMMAND_COLUMNS = ['scanner', 't_offset_sec', 'category', 'action', 'args_json']
NUMBER = r'[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?'
XY = re.compile(rf'^\(\s*({NUMBER})\s*,\s*({NUMBER})\s*\)$')


class CsvFormatError(ValueError):
    def __init__(self, code: str, message: str, row_number: int = 0):
        super().__init__(message)
        self.code, self.row_number = code, row_number

    def as_dict(self) -> dict[str, Any]:
        return {'code': self.code, 'row_number': self.row_number, 'message': str(self)}


def _fail(code, message, row=0):
    raise CsvFormatError(code, message, row)


def _read(raw: bytes):
    for encoding in ('utf-8-sig', 'cp950'):
        try:
            text = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        _fail('CSV_ENCODING', 'CSV must be UTF-8 or CP950.')
    if '\x00' in text:
        _fail('CSV_ENCODING', 'NUL bytes are not allowed; save as UTF-8 CSV.')
    reader = csv.reader(io.StringIO(text, newline=''), strict=True)
    try:
        return list(reader)
    except csv.Error as exc:
        _fail('CSV_SYNTAX', str(exc), reader.line_num)


def _finite(value, row, field):
    try:
        number = float(value)
    except (ValueError, TypeError, OverflowError):
        _fail('BAD_NUMBER', f'{field} must be a finite number.', row)
    if not math.isfinite(number):
        _fail('BAD_NUMBER', f'{field} must be a finite number.', row)
    return number


def _coordinate(value, row):
    match = XY.fullmatch(value.strip())
    if match is None:
        _fail('BAD_XY', 'G must contain (x, y), for example "(5.0, 3.0)". '
              'The CSV field must be quoted because it contains a comma.', row)
    return _finite(match[1], row, 'x'), _finite(match[2], row, 'y')


def _legacy_positions(raw):
    rows = _read(raw)
    required = ['scanner', 'intended_x_m', 'intended_y_m', 'intended_heading_deg']
    header = [v.strip() for v in rows[0]] if rows else []
    if any(header.count(k) != 1 for k in required):
        _fail('LEGACY_HEADER', 'Initial-poses CSV requires scanner, intended_x_m, '
              'intended_y_m and intended_heading_deg exactly once.')
    positions = []
    for row_no, row in enumerate(rows[1:], 2):
        if not any(v.strip() for v in row):
            continue
        if len(row) > len(header):
            _fail('LEGACY_ROW_WIDTH', 'Too many initial-pose fields.', row_no)
        data = dict(zip(header, row + [''] * (len(header)-len(row))))
        name = data['scanner'].strip()
        if not name:
            _fail('MISSING_ROBOT', 'Initial-pose scanner is empty.', row_no)
        positions.append({'row_number': row_no, 'scanner': name,
                          'x_m': _finite(data['intended_x_m'], row_no, 'x'),
                          'y_m': _finite(data['intended_y_m'], row_no, 'y'),
                          'heading_deg': _finite(data['intended_heading_deg'], row_no, 'heading')})
    return positions


def parse_registration_csv(command_bytes: bytes, initial_poses_bytes: bytes | None = None):
    """Return a JSON-serializable manifest; raise CsvFormatError on bad structure.

    Only A-E can produce entries in commands. AP-category targets are excluded
    from the initial robot roster; all other command targets require a position.
    Command vocabulary, geometry, live identity and health checks are deferred.
    """
    rows = _read(command_bytes)
    if not rows or [c.strip() for c in rows[0][:5]] != COMMAND_COLUMNS:
        _fail('COMMAND_HEADER', 'Columns A-E must be: ' + ','.join(COMMAND_COLUMNS))
    if len(rows[0]) > 8:
        _fail('EXTRA_COLUMNS', 'Only columns A-H are supported.', 1)
    commands, inline, reminders = [], [], []
    roster_open = True
    for row_no, raw in enumerate(rows[1:], 2):
        if len(raw) > 8:
            _fail('EXTRA_COLUMNS', 'Too many fields; quote the (x, y) field in G.', row_no)
        row = raw + [''] * (8-len(raw))
        cells = [v.strip() for v in row]
        name, xy = cells[5:7]
        if bool(name) != bool(xy):
            _fail('INCOMPLETE_POSITION_PAIR', 'F and G must both be filled or both blank.', row_no)
        if name:
            x, y = _coordinate(xy, row_no)
            entry = {'row_number': row_no, 'scanner': name, 'x_m': x, 'y_m': y}
            if roster_open:
                inline.append(dict(entry, heading_deg=None))
            else:
                reminders.append(dict(entry, execution_effect='none'))
        else:
            roster_open = False
        # Metadata-only roster rows are allowed. Original Excel row numbers survive.
        if not any(cells[:5]):
            if name and not roster_open:
                _fail('REMINDER_WITHOUT_COMMAND', 'A later reminder must be on a command row.', row_no)
            continue
        if not cells[0] or not cells[2] or not cells[3]:
            _fail('INCOMPLETE_COMMAND', 'Command rows require scanner, category and action.', row_no)
        if not re.fullmatch(r'\d+', cells[1]):
            _fail('BAD_OFFSET', 'B must be a nonnegative integer offset in seconds.', row_no)
        try:
            args = json.loads(cells[4] or '{}', parse_constant=lambda s: (_ for _ in ()).throw(ValueError(s)))
        except (ValueError, RecursionError) as exc:
            _fail('BAD_ARGS_JSON', f'E must be a finite JSON object: {exc}', row_no)
        if not isinstance(args, dict):
            _fail('BAD_ARGS_JSON', 'E must contain a JSON object.', row_no)
        # json.loads also accepts overflowing exponent literals; reject those recursively.
        def finite_json(value):
            if isinstance(value, float):
                return math.isfinite(value)
            if isinstance(value, dict):
                return all(finite_json(v) for v in value.values())
            if isinstance(value, list):
                return all(finite_json(v) for v in value)
            return True
        if not finite_json(args):
            _fail('BAD_ARGS_JSON', 'E contains a nonfinite number.', row_no)
        commands.append({'row_number': row_no, 'scanner': cells[0],
                         't_offset_sec': int(cells[1]), 'category': cells[2],
                         'action': cells[3], 'args': args,
                         'original_a_to_e': row[:5], 'display_time_cell': row[7]})
    if not commands:
        _fail('EMPTY_SCRIPT', 'At least one executable command row is required.')
    if initial_poses_bytes is not None and (inline or reminders):
        _fail('AMBIGUOUS_POSITIONS', 'Use either inline F/G positions or a separate initial-poses file, not both.')
    robot_targets = {c['scanner'] for c in commands if c['category'].strip().lower() not in {'ap', 'ap_meta'}}
    if initial_poses_bytes is None:
        positions = inline
        if not positions and robot_targets:
            _fail('MISSING_INITIAL_BLOCK', 'Fill the contiguous initial roster in F2/G2 onward, '
                  'or supply the existing initial-poses CSV.')
    else:
        positions = _legacy_positions(initial_poses_bytes)
    by_robot = {}
    for p in positions:
        if p['scanner'] in by_robot:
            _fail('DUPLICATE_INITIAL_ROBOT', 'Each robot must occur once in the initial roster. '
                  'Leave a blank F/G pair before later reminders.', p['row_number'])
        by_robot[p['scanner']] = p
    missing = sorted(robot_targets - by_robot.keys())
    if missing:
        _fail('MISSING_INITIAL_ROBOTS', 'Initial roster missing: ' + ', '.join(missing))
    extra = sorted(by_robot.keys() - robot_targets)
    if extra:
        _fail('UNUSED_INITIAL_ROBOTS', 'Initial roster has no robot commands for: ' + ', '.join(extra))
    warnings = []
    command_by_row = {c['row_number']: c for c in commands}
    for reminder in reminders:
        if reminder['scanner'] not in by_robot:
            _fail('UNKNOWN_REMINDER_ROBOT', 'Reminder robot is absent from initial roster.', reminder['row_number'])
        c = command_by_row[reminder['row_number']]
        reminder['t_offset_sec'] = c['t_offset_sec']
        if c['scanner'] != reminder['scanner'] or c['action'] != 'mobility.report.location':
            warnings.append({'code': 'REMINDER_REPORT_MISMATCH', 'row_number': reminder['row_number'],
                             'message': 'Reminder is not beside this robot\'s mobility.report.location command.'})
    return {'schema_version': 'experiment_csv_v1',
            'input_format': 'single_csv' if initial_poses_bytes is None else 'two_csv',
            'command_count': len(commands), 'commands': commands,
            'robot_count': len(positions), 'initial_positions': positions,
            'reentry_reminders': reminders, 'warnings': warnings,
            'start_time_cell': rows[0][7] if len(rows[0]) >= 8 else '',
            'time_source': 'NMS_assigned_t0_plus_column_B; H_is_display_only',
            'execution_authorized': False,
            'validation_scope': 'CSV structure and roster only; no safety or readiness approval'}
