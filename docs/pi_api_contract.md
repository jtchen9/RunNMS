# Pi ↔ NMS API Contract

Version: v1

This document defines the exact API contract that Pi agents are allowed to use.
Any API whose action part starts with "_" is NOT allowed for Pi.

======================================================================

Global Conventions

Time
- All timestamps use the system-wide TIME_FMT
- Pi must treat time strings as opaque
- No timezone, no UTC, no ISO-8601

Authorization Model
- Pi identifies itself by MAC address
- MAC → scanner mapping is controlled by whitelist
- Any {scanner} used by Pi must be whitelisted
- Unauthorized scanners receive HTTP 403

======================================================================

Allowed Endpoints (Pi-facing)

======================================================================

GET /health

Purpose:
Health check and basic configuration visibility

Response (example):
{
  "status": "ok",
  "redis_ok": true,
  "upload_enabled": true,
  "web_post_url_configured": true,
  "time": "2026-01-25-23:10:05",
  "time_format": "%Y-%m-%d-%H:%M:%S"
}

======================================================================

Registry & Whitelist

======================================================================

POST /registry/register

Purpose:
Register Pi with NMS using MAC address

Request:
{
  "mac": "string",
  "ip": "string | null",
  "scanner_version": "string | null",
  "capabilities": "string | null"
}

Notes:
- scanner_version and capabilities are telemetry-only
- They never affect control logic

Response:
- 200 OK, body: "scanner01"
- 403 Forbidden if MAC not whitelisted

======================================================================

Bootstrap (Init / Update)

======================================================================

GET /bootstrap/bundle/{bundle_id}

Purpose:
Download bundle ZIP

Response:
- 200 OK: ZIP file
- 404 Not Found: bundle missing
- 400 Bad Request: invalid bundle_id

----------------------------------------------------------------------

POST /bootstrap/report/{scanner}

Purpose:
Pi reports installed bundle version (telemetry only)

Request:
{
  "installed_version": "robotBundle1.0"
}

Response:
{
  "status": "ok",
  "scanner": "scanner01",
  "installed_version": "robotBundle1.0",
  "time": "YYYY-MM-DD-HH:MM:SS"
}

======================================================================

Southbound Ingest (Pi → NMS)

======================================================================

POST /ingest/{scanner}

Purpose:
Upload opaque payload (NMS does not parse content)

Headers:
- Content-Type: any

Body:
- Raw bytes

Response:
{
  "status": "accepted",
  "scanner": "scanner01",
  "queued_in": "nms:uplink:scanner01",
  "bytes": 1234,
  "sha256": "hexstring",
  "received_at": "YYYY-MM-DD-HH:MM:SS"
}

======================================================================

Commands (Polling)

======================================================================

GET /cmd/poll/{scanner}

Purpose:
Pi polls for due commands

Response:
{
  "scanner": "scanner01",
  "server_now": "YYYY-MM-DD-HH:MM:SS",
  "returned": 1,
  "commands": [
    [
      "redis-id",
      {
        "cmd_id": "uuid",
        "category": "scan",
        "action": "do_something",
        "execute_at": "YYYY-MM-DD-HH:MM:SS",
        "created_at": "YYYY-MM-DD-HH:MM:SS",
        "args_json": "{...}"
      }
    ]
  ]
}

----------------------------------------------------------------------

POST /cmd/ack/{scanner}

Purpose:
Pi acknowledges command completion

Request:
{
  "cmd_id": "uuid",
  "status": "ok | error",
  "finished_at": "YYYY-MM-DD-HH:MM:SS | null",
  "detail": "string | null"
}

Response:
{
  "status": "ok",
  "scanner": "scanner01",
  "cmd_id": "uuid",
  "finished_at": "YYYY-MM-DD-HH:MM:SS"
}

======================================================================

Summary

======================================================================

Allowed Pi endpoints:
- GET  /health
- POST /registry/register
- GET  /bootstrap/bundle/{bundle_id}
- POST /bootstrap/report/{scanner}
- POST /ingest/{scanner}
- GET  /cmd/poll/{scanner}
- POST /cmd/ack/{scanner}

All other APIs are operator-only and MUST NOT be called by Pi.
