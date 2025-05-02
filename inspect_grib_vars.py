import xarray as xr

grib_file = "/workspaces/alaska_verification/akurma.t00z.2dvaranl_ndfd_3p0.grb2"

try:
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
except Exception as e:
    print(f"❌ Failed to open GRIB file: {e}")
    exit(1)

print("\n📦 Variables in dataset:")
print(ds.data_vars)
#print(ds.u10.attrs)
# for var in ds.data_vars:
#     da = ds[var]
#     short_name = da.attrs.get("GRIB_shortName", "")
#     if short_name in ["u10", "v10"]:
#         print(f"\n✅ Found {short_name} as '{var}'")
#         print(f"📏 Units: {da.attrs.get('units', 'unknown')}")
#         print(f"🧪 Sample values (3x3):\n{da.values[0:3, 0:3]}")
