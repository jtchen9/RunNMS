# NMS health check

Scope: Redis PING, recently connected robots/APs, and a fresh snapshot from each
connected robot's front and rear cameras. No location estimation, Tag requirement,
movement, experiment registration, or state reset. This is an on-demand console
tool, not continuous monitoring or an experiment-start gate.

## Install this patch

The patch contains files for two repositories. From the NMS repository root:

    git apply --check --include='tools/*' nms-camera-health.patch
    git apply --include='tools/*' nms-camera-health.patch

From each robot's source repository root:

    git apply --check --include='robot_*' nms-camera-health.patch
    git apply --include='robot_*' nms-camera-health.patch

Deploy robot_dispatch.py and robot_health_check.py using your normal bundle
process, then restart the robot agent. No robot_agent.py, common_nms.py, NMS API,
config, or mobility-state changes are needed. Unupdated robots report UNKNOWN
(unsupported command), never PASS.

## Run on the NMS host

    python tools/nms_health_check.py

Uses the parent repository's config.py and its REDIS_URL, registry and command
keys. Run with the NMS Python environment (redis package already required by NMS).
NMS and robot clocks should be synchronized for command expiry.

    python tools/nms_health_check.py --connected-seconds 30 --timeout 90

Only devices with last_seen in the selected window are listed. A registration
entry alone is not evidence of connectivity. Default 30 seconds follows the
existing AP stale timeout value and allows three nominal 10-second polls; the
same explicit window is applied to robots. APs are listed without camera tests.
No location fields are read or displayed.

Camera PASS requires a new, nonempty image and successful snapshot helper result.
This tests capture only; no AprilTag processing or location solver is run. One
snapshot is taken per camera per invocation. Run the tool twice if desired.
Temporary images are deleted after the check. AV streaming is not interrupted:
active AV or busy mobility returns UNKNOWN (not tested). It uses the existing
camera profiles and snapshot script on each robot. Each snapshot has a 20-second
timeout. No retries are added to conceal intermittent failures.

Requests use the normal robot command stream and the existing acknowledgement
endpoint. A unique request nonce and command ID identify each fresh response.
The tool deletes only its own pending commands on timeout/interruption; an
already executing snapshot cannot be cancelled this way. Robot-side expiry
rejects delayed requests. The tool does not change any existing command or
mobility state. Running the agent check occupies its serial command loop for
up to roughly 45 seconds, so use it during maintenance before movement tests.

Exit codes: 0 = Redis/queried cameras passed (or no robots to test, explicitly
printed); 1 = a camera failed or was not tested; 2 = tool/Redis error; 130 = interrupted.

## Validation

Offline mocked tests cover both cameras passing, rear missing, snapshot timeout,
AV busy, expired requests, malformed acknowledgements, recent/stale device
classification, and an end-to-end mock Redis queue/ACK round trip. Python syntax
and patch application are checked. Actual Pi camera and live NMS validation must
be done on site.

Site acceptance: run on a working robot; disconnect its rear camera and rerun
(expect front PASS/rear FAIL); reconnect and rerun (both PASS). Confirm stale
robots are omitted. With an unavailable Redis endpoint, expect a bounded error.
