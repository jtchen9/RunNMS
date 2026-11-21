"""Read-only experiment CSV parsing. No Redis, commands, timers or robot imports.

I/J initial roster: contiguous entries from row 2, terminated by a blank pair.
F/G are operator reminders only. H is retained but never evaluated.
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
SCHEMA_VERSION = 'experiment_csv_v2'
CSV_COLUMNS = COMMAND_COLUMNS + ['reentry_robot', 'reentry_xy', '', 'initial_robot', 'initial_xy']
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


def _coordinate(value, row, column):
    match = XY.fullmatch(value.strip())
    if match is None:
        _fail('BAD_XY', f'{column} must contain (x, y), for example "(5.0, 3.0)". '
              'The CSV field must be quoted because it contains a comma.', row)
    return _finite(match[1], row, 'x'), _finite(match[2], row, 'y')


def parse_registration_csv(command_bytes: bytes, initial_poses_bytes: bytes | None = None):
    """Return a JSON-serializable manifest; raise CsvFormatError on bad structure.

    Only A-E can produce entries in commands. AP-category targets are excluded
    from the initial robot roster; all other command targets require a position.
    Command vocabulary, geometry, live identity and health checks are deferred.
    """
    if initial_poses_bytes is not None:
        _fail('OBSOLETE_POSES_FILE', 'Upload one command CSV with the initial roster in I/J; a separate poses file is no longer supported.')
    rows = _read(command_bytes)
    if not rows or [c.strip() for c in rows[0][:5]] != COMMAND_COLUMNS:
        _fail('COMMAND_HEADER', 'Columns A-E must be: ' + ','.join(COMMAND_COLUMNS))
    header = [v.strip() for v in rows[0]]
    if len(header) != 10 or header[5:7] != CSV_COLUMNS[5:7] or header[8:10] != CSV_COLUMNS[8:10]:
        _fail('SINGLE_CSV_HEADER', 'Use ten columns A-J: ' + ','.join(CSV_COLUMNS) +
              '. H1 is display-only; old F/G initial rosters are not supported.', 1)
    commands, positions, reminders = [], [], []
    roster_open = True
    for row_no, raw in enumerate(rows[1:], 2):
        if len(raw) > 10:
            _fail('EXTRA_COLUMNS', 'Only A-J are supported; quote coordinate fields containing commas.', row_no)
        row = raw + [''] * (10-len(raw))
        cells = [v.strip() for v in row]
        name, xy = cells[8:10]
        if bool(name) != bool(xy):
            _fail('INCOMPLETE_INITIAL_PAIR', 'I and J must both be filled or both blank.', row_no)
        if name:
            if not roster_open:
                _fail('INITIAL_ROSTER_GAP', 'Initial roster must be contiguous from I2/J2.', row_no)
            x, y = _coordinate(xy, row_no, 'J')
            positions.append(dict(row_number=row_no, scanner=name, x_m=x, y_m=y, heading_deg=None))
        else:
            roster_open = False
        reminder_name, reminder_xy = cells[5:7]
        if reminder_xy and not reminder_name:
            _fail('REMINDER_WITHOUT_ROBOT', 'G requires a robot ID in F.', row_no)
        if reminder_name:
            if not any(cells[:5]):
                _fail('REMINDER_WITHOUT_COMMAND', 'A re-entry reminder must be on a command row.', row_no)
            x, y = _coordinate(reminder_xy, row_no, 'G') if reminder_xy else (None, None)
            reminders.append(dict(row_number=row_no, scanner=reminder_name, x_m=x, y_m=y,
                                  execution_effect='none'))
        # Metadata-only roster rows never become commands. Keep CSV row numbers.
        if not any(cells[:5]):
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
    robot_targets = {c['scanner'] for c in commands if c['category'].strip().lower() not in {'ap', 'ap_meta'}}
    if not positions and robot_targets:
        _fail('MISSING_INITIAL_BLOCK', 'Fill the contiguous initial roster in I2/J2 onward.')
    by_robot = {}
    for p in positions:
        if p['scanner'] in by_robot:
            _fail('DUPLICATE_INITIAL_ROBOT', 'Each robot must occur once in the initial roster. '
                  'Re-entry reminders belong in F/G, not I/J.', p['row_number'])
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
        if c['scanner'] != reminder['scanner']:
            _fail('REMINDER_ROBOT_MISMATCH', 'F must name the robot commanded in A on this row.', reminder['row_number'])
    return {'schema_version': SCHEMA_VERSION,
            'input_format': 'single_csv',
            'command_count': len(commands), 'commands': commands,
            'robot_count': len(positions), 'initial_positions': positions,
            'reentry_reminders': reminders, 'warnings': warnings,
            'start_time_cell': rows[0][7] if len(rows[0]) >= 8 else '',
            'time_source': 'NMS_assigned_t0_plus_column_B; H_is_display_only',
            'execution_authorized': False,
            'validation_scope': 'CSV structure and roster only; no safety or readiness approval'}
