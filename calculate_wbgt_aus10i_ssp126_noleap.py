#!/usr/bin/env python
# coding: utf-8

import os
import sys
import calendar
import numpy as np
import xarray as xr
import dask.array as da
import pandas as pd
import glob

from coszenith import coszda, cosza
import WBGT_analytic

import subprocess
import gc
import dask

def cdo_compress(infile):
    """Compress NetCDF file using CDO."""
    tmpfile = infile + ".tmp"
    cmd = ["cdo", "-f", "nc4c", "-b", "f32", "-z", "zip_1", "--shuffle", "-setmissval,1.e+20", infile, tmpfile]
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    os.replace(tmpfile, infile)

def drop_all_bounds(ds):
    """Drop coordinates like 'time_bounds' from datasets,
    which can lead to issues when merging."""
    drop_vars = [vname for vname in ds.coords if '_bounds' in vname or '_bnds' in vname]
    return ds.drop_vars(drop_vars)

def preprocess_coords(ds):
    # Round only lon/lat to 4 decimals (adjust precision if needed)
    if "lon" in ds.coords:
        ds = ds.assign_coords(lon=ds.lon.round(1))
    if "lat" in ds.coords:
        ds = ds.assign_coords(lat=ds.lat.round(1))
    return ds
    
def open_local_datasets(base_dir, variables, version):
    """Open local NetCDF datasets for each variable into a dictionary of DataArrays."""
    dsets = dict()
    ocals = dict()
    standard_calendars = ["proleptic_gregorian", "gregorian", "standard"]  # standard calendars

    for var in variables:
        pattern = os.path.join(base_dir, var, version, '*.nc')
        print(f"Looking for files with pattern: {pattern}")
        files = sorted(glob.glob(pattern))
        if not files:
            print(f"No files found for variable: {var}")
            continue
        ds = xr.open_mfdataset(files, combine='by_coords', parallel=True, engine='h5netcdf', chunks="auto", preprocess=preprocess_coords)
        ds = drop_all_bounds(ds)

        original_calendar = ds.time.encoding.get('calendar', 'standard')
        ocals[var] = original_calendar
        
        # Convert non-standard calendars to proleptic_gregorian
        print(ds.time.encoding.get('calendar', 'standard'))

        if (ds.time.encoding.get('calendar', 'standard')) not in standard_calendars:
            ds = ds.convert_calendar("proleptic_gregorian", use_cftime=False)

        print(ds.time.encoding.get('calendar', 'standard'))

        dsets[var] = ds
    return dsets, ocals

def interp(data, year, month, original_calendar):
    """
    Interpolates data to hourly steps within the given year and month.

    Parameters:
    - data: xarray DataArray
    - year: int
    - month: int (1–12)

    Returns:
    - Interpolated and chunked DataArray
    """
    standard_calendars = ["proleptic_gregorian", "gregorian", "standard"]  # standard calendars
    start = f"{year}-{month:02d}-01T00:00:00.000000000"

    # Handle December rollover to next year
    if month == 12:
        end_year = year + 1
        end_month = 1
    else:
        end_year = year
        end_month = month + 1

    end = f"{end_year}-{end_month:02d}-01T00:00:00.000000000"

    date = np.arange(np.datetime64(start), np.datetime64(end), np.timedelta64(1, 'h'))

    if original_calendar not in standard_calendars:
        date = date[~((pd.to_datetime(date).month == 2) &
                      (pd.to_datetime(date).day == 29))]

    result = data.interp(time=date, method='linear')
    return result.chunk({'time': 24})

def vaporpres(huss, ps):
    """Calculate vapor pressure (Pa) from specific humidity and surface pressure."""
    r = huss * ((1 - huss) ** (-1))
    return ps * r * ((0.622 + r) ** (-1))

if __name__ == "__main__":
    from dask.distributed import Client

    if len(sys.argv) < 2:
        print("Usage: python calculate_wbgt_aus10i_ssp126_noleap.py <year>")
        sys.exit(1)

    try:
        year = int(sys.argv[1])
    except ValueError:
        print("Error: year must be an integer.")
        sys.exit(1)

    model = sys.argv[2]
    experiment = sys.argv[3]

    print(f"Processing year {year}, model {model}, experiment {experiment}")

    print("Starting Dask client")
    client = Client()
    print("Dask client started:", client)

    sys.path.insert(0, '/g/data/xv83/users/bxn599/ACS/wbgt/PyWBGT_analytic')
    client.run(lambda: sys.path.insert(0, '/g/data/xv83/users/bxn599/ACS/wbgt/PyWBGT_analytic'))

    BASE_DIR = f'/g/data/xv83/CCAM/output/CMIP6/DD/AUS-10i/CSIRO/{model}/ssp126/{experiment}/CCAM-v2203-SN/v1-r1/1hr'
    version = 'v20250912'

    variables = ['tas', 'huss', 'uas', 'vas', 'rsds', 'rsus', 'rlds', 'rlus', 'ps', 'rsdsdir']

    print("Opening datasets...")
    dsets, ocals = open_local_datasets(BASE_DIR, variables, version)
    if not dsets:
        print("No datasets loaded, exiting.")
        sys.exit(1)

    #years = np.unique(dsets['huss'].time.dt.year.values)
    #print(f"Years to process: {years}")

    years = [year]
    for year in years:
        print(f"\n===== Processing year {year} =====")
        tnw_list = []
        tg_list = []
        wbgt_list = []

        for month in range(1, 13):
            month_start = f"{year}-{month:02d}-01"
            last_day = calendar.monthrange(year, month)[1]
            month_end = f"{year}-{month:02d}-{last_day}"
            print(f"--> Month {month:02d}: {month_start} to {month_end}")

            year_sel = slice(month_start, month_end)

            # Select and chunk non-radiation variables (no need for previous month)
            huss = dsets['huss'].sel(time=year_sel).chunk({'time': 24}).huss
            tas = dsets['tas'].sel(time=year_sel).chunk({'time': 24}).tas
            ps = dsets['ps'].sel(time=year_sel).chunk({'time': 24}).ps
            uas = dsets['uas'].sel(time=year_sel).chunk({'time': 24}).uas
            vas = dsets['vas'].sel(time=year_sel).chunk({'time': 24}).vas
            
            # Radiation variables: include last day of previous month for interpolation continuity
            if month == 1:
                prev_month_year = year - 1
                prev_month = 12
            else:
                prev_month_year = year
                prev_month = month - 1
            
            prev_last_day = calendar.monthrange(prev_month_year, prev_month)[1]
            extended_start = f"{prev_month_year}-{prev_month:02d}-{prev_last_day}"
            extended_end = month_end  # already defined above
            
            extended_sel = slice(extended_start, month_end)
            
            rlds = dsets['rlds'].sel(time=extended_sel).chunk({'time': 24}).rlds
            rsds = dsets['rsds'].sel(time=extended_sel).chunk({'time': 24}).rsds
            rlus = dsets['rlus'].sel(time=extended_sel).chunk({'time': 24}).rlus
            rsus = dsets['rsus'].sel(time=extended_sel).chunk({'time': 24}).rsus
            rsdsdir = dsets['rsdsdir'].sel(time=extended_sel).chunk({'time': 24}).rsdsdir

            # fix rsdsdir at the start of each month
            # Step 1: Identify the target times (first of month at 00:30)
            is_first_of_month = (rsdsdir['time'].dt.day == 1)
            is_0030 = (rsdsdir['time'].dt.hour == 0) & (rsdsdir['time'].dt.minute == 30)
            target_mask = is_first_of_month & is_0030
            
            # Step 2: Shift time by one step to get previous and next values
            rsdsdir_prev = rsdsdir.shift(time=1)
            rsdsdir_next = rsdsdir.shift(time=-1)
            
            first_of_month_time = f"{year}-{month:02d}-01T00:30:00"
            print(
                f"Before fix ({first_of_month_time}):",
                float(
                    rsdsdir.sel(time=first_of_month_time)
                    .mean(dim=["lat", "lon"])
                    .compute()
                )
            )
            
            # Step 3: Interpolate linearly at target times
            rsdsdir = xr.where(
                target_mask,
                0.5 * (rsdsdir_prev + rsdsdir_next),
                rsdsdir
            )
            
            print(
                f"After fix  ({first_of_month_time}):",
                float(
                    rsdsdir.sel(time=first_of_month_time)
                    .mean(dim=["lat", "lon"])
                    .compute()
                )
            )

            rsdsdiff = rsds - rsdsdir

            # Interpolate radiation data to hourly
            rsdsinterp = interp(rsds, year, month, ocals['rsds'])
            rsusinterp = interp(rsus, year, month, ocals['rsus'])
            rldsinterp = interp(rlds, year, month, ocals['rlds'])
            rlusinterp = interp(rlus, year, month, ocals['rlus'])
            rsdsdirinterp = interp(rsdsdir, year, month, ocals['rsdsdir'])
            rsdsdiffinterp = interp(rsdsdiff, year, month, ocals['rsds'])

            # Create meshgrid for lat/lon in radians
            lon, lat = np.meshgrid(huss.lon.values, huss.lat.values)
            lat_rad = lat * np.pi / 180
            lon_rad = lon * np.pi / 180

            # Time coordinate chunked
            date = xr.DataArray(huss.time.values, dims=('time'), coords={'time': huss.time}).chunk({'time': 24})

            # Cosine zenith angles with explicit dtype and correct chunking
            cza = xr.DataArray(
                da.map_blocks(cosza, date.data, lat_rad, lon_rad, 1,
                              chunks=(24, lat.shape[0], lat.shape[1]),
                              new_axis=[1, 2], dtype=float),
                dims=huss.dims, coords=huss.coords
            ).persist()

            czda = xr.DataArray(
                da.map_blocks(coszda, date.data, lat_rad, lon_rad, 1,
                              chunks=(24, lat.shape[0], lat.shape[1]),
                              new_axis=[1, 2], dtype=float),
                dims=huss.dims, coords=huss.coords
            )
            czda = xr.where(czda <= 0, -0.5, czda).persist()

            print("Calculating vapor pressure")
            ea = xr.apply_ufunc(vaporpres, huss, ps,
                               dask="parallelized", output_dtypes=[float])

            print("Calculating wind speeds")
            wind10m = xr.apply_ufunc(lambda x, y: np.sqrt(x ** 2 + y ** 2),
                                    uas, vas,
                                    dask="parallelized", output_dtypes=[float])

            wind2m = xr.apply_ufunc(WBGT_analytic.getwind2m, wind10m, czda, rsdsinterp,
                                   dask="parallelized", output_dtypes=[float])

            f = (rsdsinterp - rsdsdiffinterp) / rsdsinterp
            f = xr.where(cza <= np.cos(89.5 / 180 * np.pi), 0, f)
            f = xr.where(f > 0.9, 0.9, f)
            f = xr.where(f < 0, 0, f)
            f = xr.where(rsdsinterp <= 0, 0, f)

            print("Calculating Tnw, Tg, WBGT")
            Tnw = xr.apply_ufunc(WBGT_analytic.calc_Tnw, tas, ea, ps, wind2m, czda,
                                 rsdsinterp, rldsinterp, rsusinterp, rlusinterp, f,
                                 dask="parallelized", output_dtypes=[float])

            Tg = xr.apply_ufunc(WBGT_analytic.calc_Tg, tas, ps, wind2m, czda,
                                rsdsinterp, rldsinterp, rsusinterp, rlusinterp, f,
                                dask="parallelized", output_dtypes=[float])

            WBGT = xr.apply_ufunc(WBGT_analytic.calc_WBGT, tas, ea, ps, wind2m, czda,
                                 rsdsinterp, rldsinterp, rsusinterp, rlusinterp, f,
                                 dask="parallelized", output_dtypes=[float])

            # Clean coordinates, assign metadata
            coords_to_drop = [coord for coord in ["h10", "h2"] if coord in Tnw.coords]
            if coords_to_drop:
                Tnw = Tnw.reset_coords(coords_to_drop, drop=True).astype("float32")
            Tnw.name = "tnw"
            Tnw.attrs["long_name"] = "Natural Wet Bulb Temperature"
            Tnw.attrs["units"] = "degK"

            if coords_to_drop:
                Tg = Tg.reset_coords(coords_to_drop, drop=True).astype("float32")
            Tg.name = "tg"
            Tg.attrs["long_name"] = "Black Globe Temperature"
            Tg.attrs["units"] = "degK"

            if coords_to_drop:
                WBGT = WBGT.reset_coords(coords_to_drop, drop=True).astype("float32")
            WBGT.name = "wbgt"
            WBGT.attrs["long_name"] = "Wet Bulb Globe Temperature"
            WBGT.attrs["units"] = "degK"

            tnw_list.append(Tnw)
            tg_list.append(Tg)
            wbgt_list.append(WBGT)

            print(f"Finished month {month:02d} for year {year}")

        # Concatenate all months for the year
        print(f"Concatenating and saving year {year} results")
        tnw_full = xr.concat(tnw_list, dim='time')
        tg_full = xr.concat(tg_list, dim='time')
        wbgt_full = xr.concat(wbgt_list, dim='time')

        chunk_shape = (1, tnw_full.sizes["lat"], tnw_full.sizes["lon"])

        encoding_params = {
            "dtype": "float32",
            "zlib": True,
            "complevel": 1,  # Increased compression level
            "shuffle": True,
            "chunksizes": chunk_shape,
            "_FillValue": 1.0e20,
        }

        tnw_encoding = {"tnw": encoding_params}
        tg_encoding = {"tg": encoding_params}
        wbgt_encoding = {"wbgt": encoding_params}
        
        # === Output paths depend on model and scenario ===
        outdir = f"/scratch/e53/bxn599/aus10i/{model}_ssp126"
        os.makedirs(outdir, exist_ok=True)

        tnw_outfile = f"{outdir}/tnw_AUS-10i_{model}_ssp126_{experiment}_CSIRO_CCAM-v2203-SN_v1-r1_1hr_{year}01010000-{year}12312300.nc"
        tg_outfile = f"{outdir}/tg_AUS-10i_{model}_ssp126_{experiment}_CSIRO_CCAM-v2203-SN_v1-r1_1hr_{year}01010000-{year}12312300.nc"
        wbgt_outfile = f"{outdir}/wbgt_AUS-10i_{model}_ssp126_{experiment}_CSIRO_CCAM-v2203-SN_v1-r1_1hr_{year}01010000-{year}12312300.nc"

#        print(f"Writing files for year {year}")
#        tnw_write = tnw_full.to_netcdf(tnw_outfile, encoding=tnw_encoding, engine='netcdf4', mode='w')
#        del tnw_full
#        gc.collect()
#        dask.compute(tnw_write)
#        cdo_compress(tnw_outfile)
#        print("Finished tnw, starting tg")

#        tg_write = tg_full.to_netcdf(tg_outfile, encoding=tg_encoding, engine='netcdf4', mode='w')
#        del tg_full
#        gc.collect()
#        dask.compute(tg_write)
#        cdo_compress(tg_outfile)
#        print("Finished tg, starting wbgt")

        wbgt_write = wbgt_full.to_netcdf(wbgt_outfile, encoding=wbgt_encoding, engine='netcdf4', mode='w')
        del wbgt_full
        gc.collect()
        dask.compute(wbgt_write)
        cdo_compress(wbgt_outfile)
        print(f"Finished writing year {year} output files")

    print("All processing complete")
