import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature

# Open GRIB file for U/V wind at 10m
grib_file = "/workspaces/alaska_verification/hrrr.t00z.wrfsfcf12.ak.grib2"
ds = xr.open_dataset(
    grib_file,
    engine="cfgrib",
    backend_kwargs={
        "filter_by_keys": {
            "typeOfLevel": "heightAboveGround",
            "stepType": "instant",
            "level": 10
        }
    }
)

u = ds['u10']*1.94384
v = ds['v10']*1.94384
lat = ds['latitude']
lon = ds['longitude'] - 360  # Convert 0–360 to -180–180 if needed

# Compute wind speed
wind_speed = np.sqrt(u**2 + v**2)

# Setup polar stereographic plot centered on Alaska
proj = ccrs.NorthPolarStereo(central_longitude=-150)

fig = plt.figure(figsize=(10, 10))
ax = plt.axes(projection=proj)
ax.set_extent([-151, -146, 60, 63], crs=ccrs.PlateCarree())

# Add map features
ax.coastlines()
ax.add_feature(cfeature.BORDERS, linestyle=':')
ax.add_feature(cfeature.LAND, facecolor='lightgray')
ax.gridlines(draw_labels=True)

# Plot wind speed
speed_plot = ax.pcolormesh(lon, lat, wind_speed, transform=ccrs.PlateCarree(), cmap='viridis')
plt.colorbar(speed_plot, ax=ax, label='Wind Speed (m/s)')

plt.title("10m Wind Speed (HRRR) in Polar Stereographic Projection")
plt.tight_layout()
plt.savefig('test.png')
