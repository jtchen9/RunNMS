"""Offline browser-preview integration checks; no real browser or robots opened."""
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[2]
AUTHOR=ROOT/'sitemap/DemoRoom/script_authoring'
sys.path.insert(0,str(AUTHOR));sys.path.insert(0,str(ROOT/'sitemap/CommonCheckers'))
import autolab_placement_map as bridge
from checker.checker_runner import validate_script

class BrowserPreview(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.folder=Path(self.tmp.name);self.csv=self.folder/'experiment_script.csv'
        self.csv.write_bytes((Path(__file__).parent/'step4_fixtures/Placement_AlphaBravo.csv').read_bytes())
        self.workbook=AUTHOR/'AutoLab_DemoRoom_Template.xlsm'
        self.before=self.workbook.read_bytes();self.original_csv=self.csv.read_bytes()
        self.report=self.folder/'validation_report.json';self.preview=self.folder/'placement_preview'
        self.policy=json.loads((AUTHOR/'config/macro_policy.json').read_text(encoding="utf-8"))
        self.hash='sha256:'+hashlib.sha256(self.original_csv).hexdigest()
    def tearDown(self):
        self.assertEqual(self.workbook.read_bytes(),self.before)
        self.assertEqual(self.csv.read_bytes(),self.original_csv)
    def report_data(self,ok=True,digest=None):
        self.report.write_text(json.dumps({'ok':ok,'script_hash':digest or self.hash}), encoding='utf-8')
    def test_checked_csv_to_embedded_image_and_new_browser_tab(self):
        r=validate_script(script_csv=self.csv,site_dir=AUTHOR.parent,common_dir=ROOT/'sitemap/CommonCheckers',report_json=self.report)
        self.assertTrue(r['ok'],r['issues'])
        with patch.object(bridge.webbrowser,'open_new_tab',return_value=True) as opened:
            rc=bridge.main(['--root',str(ROOT),'--workbook',str(self.workbook),'--csv',str(self.csv),'--preview-dir',str(self.preview),'--validation-report',str(self.report),'--checker-exit','0'])
        self.assertEqual(rc,0)
        page=self.preview/'preview.html';opened.assert_called_once_with(page.resolve().as_uri())
        text=page.read_text(encoding="utf-8");self.assertIn('>PASS</span>',text);self.assertIn('data:image/png;base64,',text)
        self.assertEqual([p.name for p in self.preview.iterdir()],['preview.html'])
    def test_fail_result_is_visible_with_map(self):
        self.report_data(False)
        with patch.object(bridge.webbrowser,'open_new_tab',return_value=True):
            rc=bridge.browser_preview(self.csv,self.workbook,self.preview,self.policy,report_path=self.report,checker_exit=1)
        self.assertEqual(rc,0)
        text=(self.preview/'preview.html').read_text(encoding="utf-8");self.assertIn('>FAIL</span>',text);self.assertIn('data:image/png;base64,',text)
    def test_old_pass_cannot_label_changed_csv_as_pass(self):
        self.report_data(True,'sha256:old')
        self.assertEqual(bridge.checker_status(self.csv,self.report,0)[0],'RESULT UNAVAILABLE')
    def test_checker_crash_cannot_reuse_previous_pass(self):
        self.report_data(True)
        self.assertEqual(bridge.checker_status(self.csv,self.report,2)[0],'CHECKER ERROR')
        self.assertEqual(bridge.checker_status(self.csv,None,None)[0],'NOT CHECKED')
    def test_render_failure_replaces_old_image_with_error_page(self):
        self.preview.mkdir();page=self.preview/'preview.html';page.write_text('STALE MAP', encoding='utf-8')
        with patch('placement_render.render_placement',side_effect=ValueError('bad <map>')):
            with patch.object(bridge.webbrowser,'open_new_tab',return_value=True) as opened:
                rc=bridge.browser_preview(self.csv,self.workbook,self.preview,self.policy)
        self.assertEqual(rc,2);opened.assert_called_once()
        text=page.read_text(encoding="utf-8");self.assertNotIn('STALE MAP',text);self.assertNotIn('data:image',text)
        self.assertIn('bad &lt;map&gt;',text)
    def test_no_open_option_for_offline_checks(self):
        with patch.object(bridge.webbrowser,'open_new_tab') as opened:
            rc=bridge.main(['--root',str(ROOT),'--workbook',str(self.workbook),'--csv',str(self.csv),'--preview-dir',str(self.preview),'--no-open'])
        self.assertEqual(rc,0);opened.assert_not_called()

if __name__=='__main__':unittest.main(verbosity=2)
