import requests

url = "https://6g-private.com:80/nms/report_status"
payload = {
    "nms_id": "DemoRoom",
    "time_local": "2026-05-04-10:50:00",
    "time_format": "%Y-%m-%d-%H:%M:%S",
    "experiment": {},
    "nms_status": {},
    "aps": [],
    "robots": [],
    "traffic_events": [],
}

r = requests.post(url, json=payload, timeout=10, verify=False)
print(r.status_code)
print(r.headers.get("content-type"))
print(r.text[:500])