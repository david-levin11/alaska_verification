"""Shared regional station discovery for observations and NDFD extraction."""


def discover_regional_stations(metadata_url, api_token, state, hfmetar="0", *, get):
    """Fetch a fresh inventory without variable, network or status restrictions.

    Alaska uses the union of mainland and western Aleutian boxes, with an
    independent local coordinate check. Hawaii retains state selection.
    A failed request never returns a partial regional inventory.
    """
    # Current station status is not a reliable indicator of observation
    # availability, including for historical backfills. Include all statuses.
    params = {
        "hfmetar": hfmetar,
        "token": api_token,
        "units": "english",
        "complete": "1",
        "format": "json"
    }
    # Live API probes return out-of-box stations with exactly +/-180 as an
    # endpoint. Split the requests just inside the dateline: the same probes
    # with +/-179.999999 returned no out-of-box stations.
    # These regional bounds include offshore and neighboring stations; they
    # are deliberately not a strict Alaska land boundary.
    selectors = (
        [{"bbox": "-179.999999,50,-129,73"}, {"bbox": "170,50,179.999999,73"}]
        if state.lower() == "ak" else [{"state": state}]
    )
    # Keep validation bounds at the true dateline, independently of the
    # request workaround. The endpoint inset is less than 0.1 m here.
    alaska_bounds = ([(-180, 50, -129, 73), (170, 50, 180, 73)]
                     if state.lower() == "ak" else [])
    stations = {}
    rejected = set()
    for selector in selectors:
        response = get(metadata_url, params={**params, **selector}, timeout=60)
        response.raise_for_status()
        metadata = response.json()
        code = str(metadata.get("SUMMARY", {}).get("RESPONSE_CODE", ""))
        if code == "2":
            continue
        if code != "1" or not isinstance(metadata.get("STATION"), list):
            raise ValueError(f"Unexpected Synoptic metadata response (code={code or 'missing'})")
        for station in metadata["STATION"]:
            # Never trust the remote bbox alone: dateline-edge requests can
            # return coordinates beyond the intended region. Validate the
            # union locally before any observation downloads are requested.
            if alaska_bounds:
                try:
                    lat = float(station["LATITUDE"])
                    lon = float(station["LONGITUDE"])
                except (KeyError, TypeError, ValueError):
                    rejected.add(station.get("STID"))
                    continue
                if not any(west <= lon <= east and south <= lat <= north
                           for west, south, east, north in alaska_bounds):
                    rejected.add(station.get("STID"))
                    continue
            stations[station["STID"]] = station

    selection = "Alaska geographic bounds" if state.lower() == "ak" else f"state={state}"
    print(f"Regional metadata: {len(stations):,} stations selected using {selection}; all statuses included")
    if rejected:
        print(f"Regional metadata: rejected {len(rejected):,} listings outside geographic bounds or with invalid coordinates")
    return list(stations.values())
