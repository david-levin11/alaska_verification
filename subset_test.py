import requests
import re
import xarray as xr

idx_url = "https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend.20250101/01/core/blend.t01z.core.f005.ak.grib2.idx"
remote_grib2_url = idx_url.replace('.idx', '')

# Download .idx file
r = requests.get(idx_url)
lines = r.text.strip().split('\n')

# Find relevant byte starts
search_terms = [":WIND:10 m above", ":WDIR:10 m above", ":GUST:10 m above"]
exprs = [re.compile(re.escape(term)) for term in search_terms]

line_starts = []

for i, line in enumerate(lines):
    for expr in exprs:
        if expr.search(line) and "10 m above ground" in line and "ens std dev" not in line:
            print(f'Found line: {line}')
            parts = line.split(':')
            start_byte = int(parts[1])
            line_starts.append((start_byte, i))

# Sort the matches
line_starts.sort()

# Calculate byte ranges
ranges = []
for j, (start_byte, line_index) in enumerate(line_starts):
    if j < len(lines) - 1:
        # End byte is start of next line (in idx) - 1
        next_line_parts = lines[line_index + 1].split(':')
        end_byte = int(next_line_parts[1]) - 1
        ranges.append(f"{start_byte}-{end_byte}")
    else:
        ranges.append(f"{start_byte}-")  # Last one to EOF
    print(f'Appending ranges: {start_byte}-{end_byte}')
# Download selected ranges
local_file = "subset.grib2"
with open(local_file, 'wb') as f_out:
    for byte_range in ranges:
        headers = {'Range': f'bytes={byte_range}'}
        r = requests.get(remote_grib2_url, headers=headers)
        if r.status_code in (200, 206):
            f_out.write(r.content)
        else:
            print(f"Failed to download range {byte_range}")

# Open dataset
ds = xr.open_dataset(local_file, engine='cfgrib')
print(ds)
