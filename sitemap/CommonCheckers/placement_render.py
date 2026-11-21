"""Read-only operator placement guide from the exported single CSV and StaticMap.

No routes, measured poses, robot commands, or experiment registration.
"""
from pathlib import Path
import hashlib
import json
import os
import tempfile
import textwrap

from checker.registration_csv import parse_registration_csv
from single_csv_authoring import reminder_issues
from route_render import read_static_map

PALETTE = ['#0072B2', '#D55E00', '#009E73', '#CC79A7', '#8C6300',
           '#168AA3', '#6F4C9B', '#555555']


def placement_records(csv_bytes, macro_policy):
    manifest = parse_registration_csv(csv_bytes)
    issues = reminder_issues(manifest, macro_policy)
    if issues:
        raise ValueError('Export the CSV again before drawing: ' +
                         '; '.join(f"row {i.get('row_number', 0)}: {i['message']}" for i in issues))
    records = []
    for p in manifest['initial_positions']:
        records.append(dict(kind='initial', scanner=p['scanner'], x_m=p['x_m'],
                            y_m=p['y_m'], csv_row=p['row_number'], time_cell=None))
    for p in sorted(manifest['reentry_reminders'],
                    key=lambda p: (p['t_offset_sec'], p['row_number'])):
        records.append(dict(kind='reentry', scanner=p['scanner'], x_m=p['x_m'],
                            y_m=p['y_m'], csv_row=p['row_number'],
                            time_cell=f"H{p['row_number']}"))
    return records


def marker_groups(records):
    groups = {}
    for record in records:
        if record['x_m'] is None:
            continue  # Blank G is a note, never a fabricated map location.
        key = (record['x_m'], record['y_m'])
        groups.setdefault(key, []).append(record)
    return groups


def short_name(scanner):
    return scanner.removeprefix('twin-scout-').capitalize()


def marker_label(records):
    parts = []
    for scanner in dict.fromkeys(r['scanner'] for r in records):
        own = [r for r in records if r['scanner'] == scanner]
        tags = (['initial'] if any(r['kind'] == 'initial' for r in own) else [])
        tags += [f"R{r['csv_row']}" for r in own if r['kind'] == 'reentry']
        parts.append(textwrap.fill(short_name(scanner) + ' ' + ', '.join(tags), 30))
    return '\n'.join(parts)


def render_placement(csv_path, workbook, output_png, macro_policy, *, dpi=240):
    from matplotlib.figure import Figure
    from matplotlib.backends.backend_agg import FigureCanvasAgg
    from matplotlib.collections import PatchCollection
    from matplotlib.patches import Rectangle, Patch
    from matplotlib.lines import Line2D

    csv_path, workbook, output_png = map(Path, (csv_path, workbook, output_png))
    output_json = output_png.with_suffix('.json')
    if output_png.suffix.lower() != '.png':
        raise ValueError('Output must be a PNG file')
    if {output_png.resolve(), output_json.resolve()} & {csv_path.resolve(), workbook.resolve()}:
        raise ValueError('Output must not overwrite an input')
    if not isinstance(dpi, int) or not 72 <= dpi <= 600:
        raise ValueError('DPI must be an integer from 72 to 600')
    csv_bytes = csv_path.read_bytes()
    records = placement_records(csv_bytes, macro_policy)
    data = read_static_map(workbook)
    # A placement guide must not silently substitute a missing/partial map.
    if data['messages']:
        raise ValueError('StaticMap cannot be rendered reliably: ' + '; '.join(data['messages']))
    for p in records:
        if p['x_m'] is not None and not (0 <= p['x_m'] < data['width'] and
                                        0 <= p['y_m'] < data['height']):
            raise ValueError(f"CSV row {p['csv_row']}: placement is outside StaticMap")

    names = list(dict.fromkeys(p['scanner'] for p in records))
    colors = {n: PALETTE[i % len(PALETTE)] for i, n in enumerate(names)}
    groups = marker_groups(records)
    # Expand the page vertically for long re-entry lists; never drop reminders.
    note_lines = 28 + len(records) * 2
    page_height = max(11.2, note_lines * .24)
    fig = Figure(figsize=(16, page_height), dpi=dpi)
    FigureCanvasAgg(fig)
    map_height = min(.78, 9.1 / page_height)
    ax = fig.add_axes([.055, .89-map_height, .60, map_height])
    panel = fig.add_axes([.69, .10, .29, .79]); panel.axis('off')
    for kind, color in [('restricted', '#dedede'), ('bump', '#f6d66b')]:
        cells = [Rectangle((x, y), data['resolution'], data['resolution'])
                 for x, y, k in data['cells'] if k == kind]
        if cells:
            ax.add_collection(PatchCollection(cells, facecolor=color, edgecolor='none', zorder=0))
    if data['overlap']:
        ax.add_collection(PatchCollection(
            [Rectangle(xy, data['resolution'], data['resolution']) for xy in data['overlap']],
            facecolor='none', edgecolor='#705600', hatch='////', linewidths=0, zorder=1))
    ax.set(xlim=(0, data['width']), ylim=(0, data['height']), xlabel='x (m)', ylabel='y (m)')
    ax.set_aspect('equal'); ax.grid(alpha=.15)
    pending = []
    for (x, y), group in groups.items():
        color = colors[group[0]['scanner']] if len({r['scanner'] for r in group}) == 1 else '#222222'
        if any(r['kind'] == 'reentry' for r in group):
            ax.scatter(x, y, marker='D', s=105, facecolors='none', edgecolors=color, linewidths=1.5, zorder=5)
        if any(r['kind'] == 'initial' for r in group):
            ax.scatter(x, y, marker='*', s=85, color=color, edgecolors='white', linewidths=.5, zorder=6)
        pending.append((x, y, marker_label(group), color))
    # Labels stay close to their point. Coincident positions share one label.
    fig.canvas.draw(); renderer = fig.canvas.get_renderer(); placed = []
    for x, y, label, color in pending:
        candidates = [(7, 7, 'left', 'bottom'), (-7, 7, 'right', 'bottom'),
                      (7, -7, 'left', 'top'), (-7, -7, 'right', 'top'),
                      (7, 19, 'left', 'bottom'), (-7, -19, 'right', 'top')]
        best = None
        for dx, dy, ha, va in candidates:
            t = ax.annotate(label, (x, y), xytext=(dx, dy), textcoords='offset points',
                            ha=ha, va=va, fontsize=8, color=color, zorder=7,
                            bbox=dict(facecolor='white', edgecolor='none', alpha=.88, pad=.7))
            box = t.get_window_extent(renderer).expanded(1.02, 1.08)
            clipped = not ax.bbox.contains(box.x0, box.y0) or not ax.bbox.contains(box.x1, box.y1)
            score = 100 * clipped + sum(box.overlaps(old) for old in placed)
            if best is None or score < best[0]: best = (score, dx, dy, ha, va, box)
            t.remove()
            if score == 0: break
        _, dx, dy, ha, va, box = best
        ax.annotate(label, (x, y), xytext=(dx, dy), textcoords='offset points',
                    ha=ha, va=va, fontsize=8, color=color, zorder=7,
                    bbox=dict(facecolor='white', edgecolor='none', alpha=.88, pad=.7))
        placed.append(box)
    # Text panel gives complete row/name details even for coincident locations.
    lines = ['INITIAL PLACEMENT', 'All robots present before preparation.', '']
    for r in records:
        if r['kind'] == 'initial':
            lines += [f"{short_name(r['scanner'])}: ({r['x_m']:g}, {r['y_m']:g})"]
    lines += ['', 'RE-ENTRY', 'R = exported CSV row; header is row 1.', 'Check column H; power on earlier.', '']
    entries = [r for r in records if r['kind'] == 'reentry']
    if not entries: lines += ['None in this script.']
    for r in entries:
        coord = ('G blank: no movement placement requested' if r['x_m'] is None
                 else f"({r['x_m']:g}, {r['y_m']:g})")
        lines += [f"{short_name(r['scanner'])} R{r['csv_row']} / {r['time_cell']}", '  ' + coord]
    lines += ['', 'PLACE SAFELY NEAR THE MARKER',
              'Keep a clear approach to the destination.',
              'Initial placement: within 2 m, clear of',
              'restrictions, with useful tags in view.',
              'Markers are intended destinations,',
              'not measured robot positions.', '',
              'Manual relocation has no travel line.',
              'Placement guidance; no safety approval.']
    panel.text(0, 1, '\n'.join(lines), transform=panel.transAxes, va='top', fontsize=9, linespacing=1.4)
    handles = [Line2D([], [], marker='*', linestyle='none', color='#333333', markersize=10, label='Initial'),
               Line2D([], [], marker='D', linestyle='none', markerfacecolor='none', color='#333333', label='Re-entry'),
               Patch(facecolor='#dedede', label='Static restriction'),
               Patch(facecolor='#f6d66b', label='Bump guard'),
               Patch(facecolor='#dedede', edgecolor='#705600', hatch='////', label='Static + bump')]
    ax.legend(handles=handles, loc='upper center', bbox_to_anchor=(.5, -.085), ncol=3, fontsize=8, frameon=False)
    fig.text(.055, .96, 'DemoRoom — robot placement guide', fontsize=20, weight='bold')
    fig.text(.055, .925, csv_path.name, fontsize=11, color='#444444')
    result = dict(schema_version='placement_guide_v1', csv_sha256=hashlib.sha256(csv_bytes).hexdigest(),
                  workbook_sha256=data['sha256'], map_sheet=data['sheet'], map_range=data['cell_range'],
                  records=records, marker_group_count=len(groups), travel_lines=0,
                  safety_checked=False, execution_authorized=False)
    output_png.parent.mkdir(parents=True, exist_ok=True)
    try:
        with tempfile.TemporaryDirectory(prefix='placement_', dir=output_png.parent) as folder:
            png = Path(folder)/'guide.png'; metadata = Path(folder)/'guide.json'
            fig.savefig(png, dpi=dpi, facecolor='white')
            metadata.write_text(json.dumps(result, indent=2)+'\n', encoding='utf-8')
            os.replace(png, output_png); os.replace(metadata, output_json)
    finally:
        fig.clear()
    return result
