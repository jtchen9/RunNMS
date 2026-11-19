"""Shared public argument normalization; does not mutate caller arguments."""
import re

_MAC = re.compile(r"(?:[0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}")


def normalize_operator_args(scanner, action, args, policy):
    if action not in ("ap.backhaul.steer", "system.shutdown"):
        return args
    if not isinstance(args, dict):
        raise ValueError("command args must be a JSON object")
    result = dict(args)
    if action == "system.shutdown":
        wait = args.get("time_to_wait", 300)
        if type(wait) is not int or wait <= 0:
            raise ValueError("time_to_wait must be a positive JSON integer; default is 300 seconds")
        if set(args) - {"time_to_wait", "_experiment"}:
            raise ValueError("unsupported shutdown arguments")
        result["time_to_wait"] = wait
        return result

    if set(args) - {"target_ap", "band", "backhaul_mac", "_experiment"}:
        raise ValueError("unsupported backhaul steering arguments")
    friendly = "target_ap" in args or "band" in args
    if friendly and "backhaul_mac" in args:
        raise ValueError("use target_ap + band OR backhaul_mac, not both")
    mapping = policy.get("backhaul_mac_by_ap_band", {})
    if friendly:
        ap, band = args.get("target_ap"), args.get("band")
        if not isinstance(ap, str) or not isinstance(band, str):
            raise ValueError("target_ap and band must both be strings")
        ap, band = ap.strip().upper(), band.strip().upper()
        if ap == str(scanner).strip().upper():
            raise ValueError("an AP cannot steer its backhaul to itself")
        mac = mapping.get(ap, {}).get(band)
        if mac is None:
            raise ValueError("unknown target_ap/band mapping; use AP1..AP6 and 2.4G, 5G or 6G")
    else:
        mac = args.get("backhaul_mac")
    if not isinstance(mac, str) or not _MAC.fullmatch(mac.strip()):
        raise ValueError("backhaul_mac must be a six-octet colon-separated MAC address")
    mac = mac.strip().lower()
    own = mapping.get(str(scanner).strip().upper(), {})
    if mac in [str(v).strip().lower() for v in own.values()]:
        raise ValueError("an AP cannot steer its backhaul to itself")
    result.pop("target_ap", None)
    result.pop("band", None)
    result["backhaul_mac"] = mac
    return result
