"""Called by Excel to draw an operator guide; never registers an experiment."""
import argparse
import base64
import hashlib
import html
import json
from pathlib import Path
import sys
import tempfile
import webbrowser


def checker_status(csv_path, report_path=None, checker_exit=None):
    """Never display an old PASS for a different CSV or failed checker run."""
    if checker_exit not in (0, 1):
        return 'CHECKER ERROR' if checker_exit is not None else 'NOT CHECKED', ''
    try:
        report = json.loads(Path(report_path).read_text(encoding='utf-8-sig'))
        digest = 'sha256:' + hashlib.sha256(Path(csv_path).read_bytes()).hexdigest()
        if report.get('script_hash') != digest:
            return 'RESULT UNAVAILABLE', 'The checker report does not match this CSV.'
        if checker_exit == 0 and report.get('ok') is True:
            return 'PASS', ''
        if checker_exit == 1 and report.get('ok') is False:
            return 'FAIL', 'Fix the errors shown in the workbook validation feedback.'
        return 'RESULT UNAVAILABLE', 'The checker exit code and report disagree.'
    except (OSError, ValueError, TypeError):
        return 'RESULT UNAVAILABLE', 'The current checker report could not be read.'


def browser_preview(csv_path, workbook, folder, policy, *, report_path=None,
                    checker_exit=None, open_browser=True):
    """One cached HTML page with an embedded image; no PNG download/save step."""
    from placement_render import render_placement
    folder = Path(folder).resolve()
    page = folder/'preview.html'
    if page in {Path(csv_path).resolve(), Path(workbook).resolve()}:
        raise ValueError('Preview must not overwrite an input')
    folder.mkdir(parents=True, exist_ok=True)
    status, note = checker_status(csv_path, report_path, checker_exit)
    error = ''
    picture = ''
    try:
        with tempfile.TemporaryDirectory(prefix='autolab_placement_') as tmp:
            png = Path(tmp)/'placement.png'
            render_placement(csv_path, workbook, png, policy)
            encoded = base64.b64encode(png.read_bytes()).decode('ascii')
            picture = '<img alt="Robot initial and re-entry placement guide" src="data:image/png;base64,' + encoded + '">'
    except Exception as exc:
        error = str(exc)
    # Replace the prior page even on rendering failure: never show a stale map.
    color = '#12632d' if status == 'PASS' else '#a02920'
    content = ('<!doctype html><html><head><meta charset="utf-8">'
               '<meta http-equiv="Cache-Control" content="no-store">'
               '<title>DemoRoom placement preview</title><style>'
               'body{font:16px sans-serif;margin:24px;color:#222}'
               'img{display:block;width:100%;height:auto} '
               '.error{color:#a02920} .status{font-weight:bold}</style></head><body>'
               '<h1>Robot placement preview</h1>'
               f'<p>{html.escape(Path(csv_path).name)} — CommonCheckers: '
               f'<span class="status" style="color:{color}">{status}</span></p>'
               f'<p>{html.escape(note)}</p>'
               '<p>Initial robots: before preparation. Re-entry Rn: check Hn and power on early. '
               'Manual relocation has no travel line.</p>')
    if error:
        content += '<p class="error">Placement preview failed: ' + html.escape(error) + '</p>'
    else:
        content += picture
    content += '</body></html>'
    page.write_text(content, encoding='utf-8')
    opened = True
    if open_browser:
        try:
            opened = webbrowser.open_new_tab(page.as_uri())
        except Exception:
            opened = False
    print(f'Placement preview: {page}')
    if not opened:
        print('Could not open the browser automatically; open the preview path above.', file=sys.stderr)
    if error:
        print('Placement preview failed: ' + error, file=sys.stderr)
    return 2 if error or not opened else 0


def main(argv=None):
    parser = argparse.ArgumentParser()
    for name in ('root', 'workbook', 'csv'):
        parser.add_argument('--' + name, required=True)
    output = parser.add_mutually_exclusive_group(required=True)
    output.add_argument('--output')
    output.add_argument('--preview-dir')
    parser.add_argument('--validation-report')
    parser.add_argument('--checker-exit', type=int)
    parser.add_argument('--no-open', action='store_true')
    args = parser.parse_args(argv)
    root = Path(args.root).resolve()
    sys.path.insert(0, str(root/'sitemap/CommonCheckers'))
    try:
        from placement_render import render_placement
        policy = json.loads((root/'sitemap/DemoRoom/script_authoring/config/macro_policy.json').read_text(encoding='utf-8'))
        if args.preview_dir:
            return browser_preview(args.csv, args.workbook, args.preview_dir, policy,
                                   report_path=args.validation_report,
                                   checker_exit=args.checker_exit,
                                   open_browser=not args.no_open)
        result = render_placement(args.csv, args.workbook, args.output, policy)
        print(f"Placement guide: {len(result['records'])} entries; {args.output}")
        return 0
    except Exception as exc:
        print(f'Placement guide failed: {exc}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
