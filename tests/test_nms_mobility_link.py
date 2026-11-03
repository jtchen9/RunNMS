"""Exercise the deployed handler bodies without starting NMS or connecting to Redis."""
import ast
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace as NS
import unittest
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[1]


def funcs(module, names, env):
    tree = ast.parse((ROOT / (module + '.py')).read_text(encoding='utf-8-sig'))
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert len(nodes) == len(names)
    exec(compile('from __future__ import annotations\n' + ast.unparse(ast.Module(nodes, [])),
                 module, 'exec'), env)
    return env


class GraceTests(unittest.TestCase):
    def setUp(self):
        self.db = {}
        self.r = Mock()
        self.r.hget.side_effect = lambda k,f: self.db.get((k,f))
        self.r.hsetnx.side_effect = lambda k,f,v: self.db.setdefault((k,f),v)
        self.r.hdel.side_effect = lambda k,f: self.db.pop((k,f),None)
        self.cfg = NS(r=self.r, MOBILITY_LINK_GRACE_SEC=120,MOBILITY_POLL_STALE_SEC=30,
                      KEY_MOBILITY_CMD_STREAM='script',CMD_EXPIRE_SEC=3600,
                      key_scanner_meta=lambda s:s,key_cmd_stream=lambda s:s)
        self.u=Mock()
        self.u.parse_local_dt.side_effect=datetime.fromisoformat
        self.u.local_ts.return_value='2026-11-03 10:00:00'
        self.u._hget.return_value='2026-11-03 09:59:00'
        self.now=1000
        self.link=funcs('m8mobility_link',{'poll_fresh','disposition'},
                        dict(config=self.cfg,utility=self.u,time=NS(time=lambda:self.now)))

    def test_fixed_deadline(self):
        gate=self.link['disposition']
        self.assertEqual(gate('b','bt','s1'),'wait')
        self.now+=119
        self.assertEqual(gate('b','bt','s1'),'wait')
        self.now+=1
        self.assertEqual(gate('b','bt','s1'),'expired')

    def test_held_row_then_release(self):
        issue=Mock(return_value={'status':'ok'})
        env=dict(config=self.cfg,json=__import__('json'),key_time=lambda s:s,
                 _get_state=lambda s:'idle',S0_IDLE='idle',_load_stop=lambda:{},
                 on_command_issued=issue,link=NS(disposition=self.link['disposition']))
        dispatch=funcs('m8mobility',{'_dispatch_due_mobility_command'},env)['_dispatch_due_mobility_command']
        row={'scanner':'b','action':'mobility.move','args_json':'{}'}
        dispatch('1-0',row);dispatch('1-0',row)
        issue.assert_not_called(); self.r.xdel.assert_not_called()
        self.u._hget.return_value=self.u.local_ts.return_value
        dispatch('1-0',row)
        issue.assert_called_once_with('b','mobility.move',{})
        self.r.xdel.assert_called_once_with('script','1-0')

    def test_delivery_once_and_no_stale_move(self):
        read=funcs('m4Commands',{'_collect_due_commands'},dict(config=self.cfg,utility=self.u,
                   m8mobility=NS(should_block_command=lambda _:False)))['_collect_due_commands']
        self.r.xrange.return_value=[('1-0',dict(execute_at=self.u.local_ts.return_value,
            mobility_s1_managed='1',action='mobility.turn_move_turn.forward'))]
        self.r.xdel.side_effect=[1,0,1]
        self.assertEqual(len(read('b',20,'2026-11-03 10:00:10')['commands']),1)
        self.assertFalse(read('b',20,'2026-11-03 10:00:10')['commands'])
        self.assertFalse(read('b',20,'2026-11-03 10:00:31')['commands'])

    def test_s1_offline_then_late_report(self):
        timer,process=Mock(),Mock(return_value={'status':'ok'})
        env=dict(config=self.cfg,_acquire_s1_lock=lambda s:'owner',_release_s1_lock=Mock(),
             _get_state=lambda s:'s1',S1_WAITING_REPORT='s1',utility=self.u,key_time=lambda s:s,
             _s1_has_fresh_report=Mock(return_value=(False,'')),_site_macro_active=lambda s:False,
             link=NS(disposition=lambda *args:'wait'),_start_s1_timer=timer,_cancel_s1_timer=Mock(),
             _s1_report_matches_expected_command=lambda s:(True,''),enter_s1waiting_report_on_report=process)
        self.u._hget.return_value='token'
        resolve=funcs('m8mobility_state',{'process_s1_event'},env)['process_s1_event']
        self.assertEqual(resolve('b','timeout','token')['status'],'waiting')
        timer.assert_called_once_with('b',delay=2)
        self.u._hset_many.assert_not_called()
        env['_s1_has_fresh_report'].return_value=(True,'')
        self.assertEqual(resolve('b','report')['status'],'ok')
        process.assert_called_once_with('b')

    def test_online_timeout_keeps_original_recovery_and_offline_expiry_stops(self):
        self.u._hget.side_effect=lambda key,field,default: 't' if field=='s1_timer_token' else '0'
        setstate=Mock()
        env=dict(config=self.cfg,_acquire_s1_lock=lambda s:'owner',_release_s1_lock=Mock(),_get_state=lambda s:'s1',
            S1_WAITING_REPORT='s1',S7_STOPPED='s7',S4_WAITING_LOCATION_RETRY='s4',
            S1_REPORT_TIMEOUT_SEC=90,utility=self.u,key_time=lambda s:s,key_state=lambda s:s,
            _s1_has_fresh_report=lambda s:(False,''),_site_macro_active=lambda s:False,
            link=NS(disposition=lambda *args:'ready'),_cancel_s1_timer=Mock(),_set_state=setstate,
            _unexpected_event_sum=lambda **kw:sum(kw.values()),UNEXPECTED_EVENT_SUM_LIMIT=2,
            run_state_machine=lambda s:{'status':'recover'},s7stopped=lambda s:{'status':'stopped'})
        resolve=funcs('m8mobility_state',{'process_s1_event'},env)['process_s1_event']
        self.assertEqual(resolve('b','timeout','t')['status'],'recover')
        self.assertEqual(self.u._hset_many.call_args.args[1]['busy_count'],'1')
        env['link'].disposition=lambda *args:'expired'
        self.u._hset_many.reset_mock()
        self.assertEqual(resolve('b','timeout','t')['status'],'stopped')
        self.u._hset_many.assert_not_called()

    def test_monitor_scopes_to_active_mobility_participants_without_due_command(self):
        import json
        live = dict(state='registered',start_at='10:00',end_at='11:00',
                    mobility_scanners_json='["bravo","delta"]')
        rows = [('1',dict(live,state='completed')),('2',dict(live,start_at='10:45')),
                ('3',live)]
        self.r.xrange.return_value=rows
        self.cfg.KEY_EXPERIMENT_REGISTRY='experiments'
        check=funcs('m8mobility',{'_check_participant_links'},dict(
            config=self.cfg,link=NS(disposition=self.link['disposition']),key_time=lambda s:s,
            _load_stop=lambda:{},json=json))['_check_participant_links']
        check('10:15')
        self.assertEqual(set(self.db),{('bravo','communication_deadline'),('delta','communication_deadline')})
        self.now+=40
        self.assertEqual(self.link['disposition']('bravo','bravo','communication_deadline'),'wait')
        self.assertEqual(float(self.db['bravo','communication_deadline']),1120)

if __name__=='__main__': unittest.main()
