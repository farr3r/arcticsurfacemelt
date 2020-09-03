import xarray as xr
import numpy as np
import os
import glob

def monthly_averages(yr):
    os.chdir(# /insert/path/to/app-x/data)
    files = sorted(glob.glob('Polar-APP-X_v02r00_Nhem_1400_d*.nc'))
    dset = xr.open_mfdataset(files)
    lat = dset.latitude.load()
    lon = dset.longitude.load()
    albedo = dset.cdr_surface_albedo.load()
    swd = dset.cdr_surface_downwelling_shortwave_flux.load()
    lwu = dset.cdr_surface_upwelling_longwave_flux.load()
    lwd = dset.cdr_surface_downwelling_longwave_flux.load()
    dset.close()
    
    short = (1 - albedo) * swd
    long = lwd - lwu
    net = short + long
    
    short_mean = short.groupby('time.month').mean(dim='Time')
    long_mean = long.groupby('time.month').mean(dim='Time')
    net_mean = net.groupby('time.month').mean(dim='Time')
    
    dset2 = xr.Dataset()
    dset2['lon'] = lon
    dset2['lat'] = lat
    dset2['short'] = short_mean
    dset2['long'] = long_mean
    dset2['net'] = net_mean
    dset2 = dset2.assign_coords({'year':np.array([yr])})
    
    return dset2

for yr in range(1982, 2020):
    dset2 = monthly_averages(yr)
    dset2.to_netcdf(# /insert/path/to/save/files/to)
    dset2.close()
    print(yr)