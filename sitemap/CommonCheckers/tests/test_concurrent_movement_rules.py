import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from checker.timeline_rules import check_global_moving_command_spacing as check
from checker.concurrent_movement_rules import segment_distance
from checker.script_model import InitialPose, ScriptRow

POLICY = {'moving_mobility_actions': ['mobility.move', 'mobility.in2out'],
          'global_min_seconds_between_moving_mobility_commands': 180,
          'concurrent_movement_min_path_separation_m': 1.0}

def pose(name, x, y):
    return InitialPose(2, name, x, y, 0, .15, 10)

def move(n, name, t, x, y):
    return ScriptRow(n, name, t, 'mobility', 'mobility.move', {'x_m': x, 'y_m': y})

class ConcurrencyTests(unittest.TestCase):
    def test_parallel_boundary(self):
        for distance, fail in [(1., False), (.999999, True), (1.01, False)]:
            with self.subTest(distance=distance):
                poses = {'a': pose('a', 2, 2), 'b': pose('b', 2, 2+distance)}
                rows = [move(2,'a',180,3,2),move(3,'b',180,3,2+distance)]
                self.assertEqual(bool(check(rows,POLICY,poses)), fail)

    def test_crossing_time_boundary(self):
        poses = {'a': pose('a',2,3),'b':pose('b',3,2)}
        for gap in [0, 179, 180, 181]:
            rows=[move(2,'a',180,4,3),move(3,'b',180+gap,3,4)]
            self.assertEqual(bool(check(rows,POLICY,poses)), gap < 180)

    def test_nonadjacent_pairs(self):
        poses={'a':pose('a',2,3),'b':pose('b',8,8),'c':pose('c',3,2)}
        rows=[move(2,'a',180,4,3),move(3,'b',200,8,9),move(4,'c',210,3,4)]
        issues=check(rows,POLICY,poses)
        self.assertEqual([(i['previous_row_number'],i['row_number']) for i in issues],[(2,4)])

    def test_same_robot_with_interleaved_other(self):
        poses={'a':pose('a',2,2),'b':pose('b',8,8)}
        rows=[move(2,'a',180,3,2),move(3,'b',200,8,9),move(4,'a',359,4,2)]
        self.assertEqual(len(check(rows,POLICY,poses)),1)
        rows[-1].t_offset_sec=360
        self.assertEqual(check(rows,POLICY,poses),[])

    def test_macro_crossing(self):
        poses={'a':pose('a',9.1,4.399),'b':pose('b',8,5.4)}
        macro={'macros':{'mobility.in2out':dict(start_x_m=9.1,start_y_m=4.399,target_heading_deg=90,distance_m=2)}}
        rows=[ScriptRow(2,'a',180,'mobility','mobility.in2out',{}),move(3,'b',200,10,5.4)]
        self.assertEqual(check(rows,POLICY,poses,macro)[0]['actual_path_separation_m'],0)

    def test_geometry_edge_cases(self):
        self.assertEqual(segment_distance(((0,0),(2,0)),((1,0),(3,0))),0)
        self.assertEqual(segment_distance(((0,0),(0,0)),((1,0),(1,2))),1)
        self.assertEqual(segment_distance(((0,0),(1,0)),((2,0),(3,0))),1)

    def test_unknown_geometry_fails_closed(self):
        rows=[move(2,'a',180,3,2),move(3,'b',180,8,8)]
        self.assertEqual(len(check(rows,POLICY)),1)

    def test_pose_propagation(self):
        poses={'a':pose('a',2,2),'b':pose('b',4,1)}
        rows=[move(2,'a',180,4,2),move(3,'a',360,6,2),move(4,'b',360,4,3)]
        self.assertEqual(check(rows,POLICY,poses)[0]['previous_row_number'],3)

    def test_input_order_does_not_change_result(self):
        poses={'a':pose('a',2,3),'b':pose('b',3,2)}
        rows=[move(2,'a',180,4,3),move(3,'b',180,3,4)]
        self.assertEqual(check(rows,POLICY,poses),check(rows[::-1],POLICY,poses))

if __name__ == '__main__':
    unittest.main()
