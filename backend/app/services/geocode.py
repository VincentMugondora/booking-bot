import json
import urllib.request
import urllib.parse

USER_AGENT = "HustlrBot/1.0 (contact: support@hustlr.app)"


def reverse_geocode(lat: float, lng: float, timeout: int = 4) -> str | None:
    """Reverse-geocode coordinates to a human-friendly label using Nominatim.
    Returns a display name, or None on failure.
    """
    try:
        params = {
            "format": "jsonv2",
            "lat": str(lat),
            "lon": str(lng),
            "zoom": "14",
            "addressdetails": "0",
        }
        url = "https://nominatim.openstreetmap.org/reverse?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("display_name")
    except Exception:
        return None
