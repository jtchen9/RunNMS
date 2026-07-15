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

### bump_guard_zone

The bump guard zone is defined in:

```text
script_authoring/config/bump_guard_zones.json
```

Normal `mobility.move` paths must not cross this rectangle. Crossing is legal
only through:

```text
mobility.in2out
mobility.out2in
```

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
