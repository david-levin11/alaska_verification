import os
import requests
import re
import xarray as xr

def download_subset(remote_url, local_filename, search_strings, require_all_matches=True,
                     required_phrases=None, exclude_phrases=None):
    """
    Download a subset of a GRIB2 file based on .idx entries matching search_strings.

    Args:
        remote_url (str): Full URL to remote .grib2 file (not .idx)
        local_filename (str): Local path to save subset
        search_strings (list of str): Substring search matches (e.g., ":WIND:10 m above")
        require_all_matches (bool): If True, require all search_strings to match
        required_phrases (list of str, optional): Must appear in matching lines
        exclude_phrases (list of str, optional): If present in line, skip
    Returns:
        local_filename (str) if successful, None otherwise
    """
    print(f"  > Downloading subset for {os.path.basename(remote_url)}")
    print(f"🧪 Search strings: {search_strings}")

    os.makedirs(os.path.dirname(local_filename), exist_ok=True)

    idx_url = remote_url + ".idx"
    r = requests.get(idx_url)
    if not r.ok:
        print(f'     ❌ Could not get index file: {idx_url} ({r.status_code} {r.reason})')
        return None

    lines = r.text.strip().split('\n')
    exprs = {s: re.compile(re.escape(s)) for s in search_strings}

    matched_ranges = {}
    matched_vars = set()

    for n, line in enumerate(lines, start=1):
        if exclude_phrases and any(phrase in line for phrase in exclude_phrases):
            continue

        if required_phrases and not all(phrase in line for phrase in required_phrases):
            continue

        for search_str, expr in exprs.items():
            if expr.search(line):
                matched_vars.add(search_str)
                parts = line.split(':')
                rangestart = int(parts[1])

                # End byte: either next line's start byte, or EOF
                if n < len(lines):
                    parts_next = lines[n].split(':')
                    rangeend = int(parts_next[1]) - 1
                else:
                    rangeend = ''

                matched_ranges[f'{rangestart}-{rangeend}' if rangeend else f'{rangestart}-'] = line

    # Check matches
    if require_all_matches and len(matched_vars) != len(search_strings):
        print(f'      ⚠️ Not all variables matched! Found: {matched_vars}. Skipping {remote_url}.')
        return None

    if not matched_ranges:
        print(f'      ❌ No matches found for {search_strings}')
        return None

    # Now download the matching byte ranges
    with open(local_filename, 'wb') as f_out:
        for byteRange in matched_ranges.keys():
            headers = {'Range': f'bytes=' + byteRange}
            r = requests.get(remote_url, headers=headers)
            if r.status_code in (200, 206):
                f_out.write(r.content)
            else:
                print(f"      ❌ Failed to download byte range {byteRange}")
                return None

    print(f'      ✅ Downloaded [{len(matched_ranges)}] fields from {os.path.basename(remote_url)} → {local_filename}')
    return local_filename if os.path.exists(local_filename) else None

if __name__ == "__main__":
    urls = ['https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend.20250101/01/core/blend.t01z.core.f005.ak.grib2', 'https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend.20250101/07/core/blend.t07z.core.f005.ak.grib2', 'https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend.20250101/13/core/blend.t13z.core.f005.ak.grib2', 'https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend.20250101/19/core/blend.t19z.core.f005.ak.grib2', 'https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend.20250102/01/core/blend.t01z.core.f005.ak.grib2']
    for url in urls:
        print(f'Now processing {url}...')
        local_file = download_subset(
            remote_url=url,
            local_filename=f"nbm/{url.split('/')[-1]}",
            search_strings=[":WIND:10 m above", ":WDIR:10 m above", ":GUST:10 m above"],
            require_all_matches=True,
            required_phrases=["10 m above ground"],
            exclude_phrases=["ens std dev"]
        )

        ds = xr.open_dataset(local_file, engine='cfgrib')
        print(ds)