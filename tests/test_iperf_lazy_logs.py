import importlib.util
import io
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('lazy_subject',ROOT/'m7TrafficServer.py')
M=importlib.util.module_from_spec(spec)
with patch.dict(sys.modules,{'config':types.SimpleNamespace()}):spec.loader.exec_module(M)
class LogTests(unittest.TestCase):
    def test_unused_port_creates_no_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'port-5200.jsonl';log=M.PortLog(path)
            log.start(io.BytesIO(b''));log.close()
            self.assertFalse(path.exists());self.assertTrue(log.closed)
    def test_output_preserved_and_restarts_append(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'port-5224.jsonl'
            for data in [b'{"event":"start"}\n',b'{"event":"end"}\n']:
                log=M.PortLog(path);log.start(io.BytesIO(data));log.close()
                self.assertEqual(log.error,'')
            self.assertEqual(path.read_bytes(),b'{"event":"start"}\n{"event":"end"}\n')
    def test_failure_before_start_creates_no_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'port-5200.jsonl';log=M.PortLog(path);log.close()
            self.assertFalse(path.exists());self.assertTrue(log.closed)
if __name__=='__main__':unittest.main()
