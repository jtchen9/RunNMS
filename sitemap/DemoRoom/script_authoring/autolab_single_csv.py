"""Excel bridge: prepare a one-file export or validate an import. No execution."""
import argparse
import json
from pathlib import Path
import sys

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--root',required=True)
    parser.add_argument('--input',required=True)
    parser.add_argument('--output',required=True)
    parser.add_argument('--validate-only',action='store_true')
    args=parser.parse_args()
    root=Path(args.root)
    sys.path.insert(0,str(root/'sitemap/CommonCheckers'))
    from checker.registration_csv import parse_registration_csv
    from single_csv_authoring import prepare_export
    try:
        raw=Path(args.input).read_bytes()
        if args.validate_only:
            parse_registration_csv(raw);result=raw
        else:
            policy=json.loads((root/'sitemap/DemoRoom/script_authoring/config/macro_policy.json').read_text(encoding='utf-8-sig'))
            result=prepare_export(raw,policy)
        target=Path(args.output);target.write_bytes(result)
        return 0
    except Exception as exc:
        print(f'{type(exc).__name__}: {exc}',file=sys.stderr)
        return 2

if __name__=='__main__':raise SystemExit(main())
