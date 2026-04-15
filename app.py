from fastapi import FastAPI
import uvicorn
import asyncio

import m0Health
import m1Registry
import m2Bootstrap
import m3Ingest
import m4Commands
import m5Northbound
import m6AP
import m7Traffic
import m8Mobility
import m9Admin

# ==========================
# App
# ==========================
app = FastAPI(
    title="Wi-Fi NMS (Control-plane + Pass-through Relay)",
    description=(
        "This NMS is intentionally *not* a data processor.\n\n"
        "- Receives opaque scan payloads from scanners and queues them for uplink\n"
        "- Manages scanner whitelist + registry\n"
        "- Provides bootstrap bundles (ZIP) for initializing/updating scanners\n"
        "- Provides command enqueue/poll/ack (extendible: scan/robot/video/audio)\n"
    ),
)

app.include_router(m0Health.router)
app.include_router(m1Registry.router)
app.include_router(m2Bootstrap.router)
app.include_router(m3Ingest.router)
app.include_router(m4Commands.router)
app.include_router(m6AP.router)
app.include_router(m7Traffic.router)
app.include_router(m8Mobility.router)
app.include_router(m9Admin.router)

# =============================
# Auto-flush background task
# =============================
@app.on_event("startup")
async def _startup():
    asyncio.create_task(m5Northbound._northbound_loop())
    asyncio.create_task(m5Northbound._status_loop())
    asyncio.create_task(m7Traffic._traffic_loop())
    

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)