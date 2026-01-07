from fastapi import FastAPI
from pydantic import BaseModel
import redis
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM_MAXLEN = int(os.getenv("STREAM_MAXLEN", "2000"))

r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

app = FastAPI(title="Wi-Fi NMS")

class ScanResult(BaseModel):
    scanner: str
    bssid: str
    ssid: str
    freq: float
    signal: float

@app.post("/scans/")
def post_scan(result: ScanResult):
    """Store scan result into Redis stream."""
    key = f"scanner:{result.scanner}:scans"
    r.xadd(key, result.dict(), maxlen=STREAM_MAXLEN, approximate=True)
    return {"status": "ok", "stored_in": key}

@app.get("/latest/{scanner}")
def latest_scan(scanner: str):
    """Fetch the most recent scan of a given scanner."""
    key = f"scanner:{scanner}:scans"
    data = r.xrevrange(key, count=1)
    return data or {"error": "no data"}

@app.get("/last/{scanner}/{n}")
def last_n_scans(scanner: str, n: int):
    """Fetch last n scans of a given scanner."""
    key = f"scanner:{scanner}:scans"
    data = r.xrevrange(key, count=n)
    return data
