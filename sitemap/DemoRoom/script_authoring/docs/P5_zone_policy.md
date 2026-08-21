# DemoRoom P5 Zone Policy

This folder separates the different kinds of safety zones used by the planned
path checker.

## Zone types

### blocked_zone

Normal robot paths must not enter or cross this zone.

### charging_zone

A robot may start or end inside its own charging/rest zone. This is needed
because charging locations may be inside areas that are otherwise marked as
restricted.

Charging zones are not treated as open walking space. Passing through another
robot's charging zone remains forbidden by default.

### ramp restriction zone

The single authoritative ramp geometry and macro policy is defined in:

```text
script_authoring/config/macro_policy.json
```

Normal `mobility.move` paths starting outside must not touch or enter this
rectangle. A robot already in a landing buffer may leave through any side if
the endpoint is outside and the segment does not touch the bump core. Crossing
the core is legal only through:

```text
mobility.in2out
mobility.out2in
```

The XLSM source script stages these macros with reserved coordinates:

```text
IN2OUT, IN2OUT -> (9.06, 4.30) -> mobility.in2out
OUT2IN, OUT2IN -> (9.06, 6.10) -> mobility.out2in
```

CommonCheckers requires the matching staging move to be the preceding mobility
command for the same robot. It resolves the constant before applying all normal
`mobility.move` safety checks and emits only numeric coordinates for NMS use.

Preflight requires the exact configured launch point. Runtime uses a fresh
AprilTag pose with independent X/Y tolerances. The initial crossing command
uses `bump_crossing_up` or `bump_crossing_down`; any post-cross correction is a
normal mobility command and does not carry a crossing profile.

## Current rough charging centers

```text
Charging/rest centers:

Alpha   (1.8, 4.8), heading 270
Bravo   (2.4, 4.8), heading 270
Charlie (3.0, 4.8), heading 270
Delta   (3.6, 4.8), heading 270

Script initial poses, after manually moving robots out before experiment:

Alpha   (1.8, 4.4), heading 270
Bravo   (2.4, 4.4), heading 270
Charlie (3.0, 4.4), heading 270
Delta   (3.6, 4.4), heading 270
```

Charging/rest centers and script initial poses are separate values. Both can be fine tuned later.
