# Alternate-view recovery (Step 5)

Run s8_step5_check.py in VS Code from this checkout after building the Step 4 LUT.
It reads the installed DemoRoom tables and runtime map. It uses an in-memory queue,
the archived failure input, and simulated post-stop reports and solver output.
It does not connect to the NMS or a robot. The Phase B golden-angle check assumes
the November 12 DemoRoom tables; review this fixture if the tag map or LUT changes.

Both MOBILITY_S8_ENABLED and MOBILITY_S8_VIEW_ENABLED must be Python True in
the NMS config to activate this branch. Missing settings default to off. The
Step 3 landing switch is independent. Restart the NMS after changing settings,
before registration, with no experiment active. No robot code or CSV change.

One LOW AprilTag first-correction failure after a completed ordinary forward
movement of at most 0.30 m is eligible. A HIGH pre-move AprilTag pose must have
been recorded at planning. Rules use its grid cell and a post-move heading
estimate (pre-move heading plus the commanded turns); if the LOW report's
heading differs by more than 25 degrees, stop. This is not absolute heading
feedback. Only turns between 30 and 135 degrees with at least two proposed new
tags are eligible. Runtime static, peer and ramp checks can veto the turn.

S8 owns only a rotation and an explicit mobility.report.location. Both stay
tied to the parent trace. A child is claimed once at poll; a lost reply is not
replayed. On a completed report, both camera filenames must carry timestamps
inside that robot command's receive/finish interval; tag evidence must point
to those captures. A fresh HIGH solve must agree with the expected heading
within 25 degrees, be within 0.30 m of the modeled endpoint and 0.45 m of
the trusted start, and use at least two predicted tags absent from the failed
observation. These are conservative initial policy values, not empirical
accuracy guarantees. S5 then re-evaluates the original target with its
ordinary correction limits. A failure or 120-second episode timeout goes to
S7; no second recovery for that intent.

For the first supervised trial, check the trace for s8_view_phase in order:
turn_queued, turn_sent, report_queued, report_sent, verified. Sent means
claimed for a poll reply, not confirmed physical execution. Compare the robot's
actual heading and reported tag IDs with the prediction before unattended use.
If S7 stops, preserve the original stop reason plus its recovery detail.
