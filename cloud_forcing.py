import xarray as xr
import numpy as np
import os
import glob

def monthly_averages(yr):
    os.chdir(# /insert/path/to/app-x/data)
    files = sorted(glob.glob('Polar-APP-X_v02r00_Nhem_1400_d*.nc'))
    dset = xr.open_mfdataset(files, concat_dim='Time', combine='nested')
    lat = dset.latitude.load()
    lon = dset.longitude.load()
    cloud_long = dset.surface_longwave_cloud_radiative_forcing.load()
    cloud_short = dset.surface_shortwave_cloud_radiative_forcing.load()
    dset.close()
    
    cloud_net = cloud_long + cloud_short
    
    cloud_long = cloud_long.groupby('time.month').mean(dim='Time')
    cloud_short = cloud_short.groupby('time.month').mean(dim='Time')
    cloud_net = cloud_net.groupby('time.month').mean(dim='Time')
    
    dset2 = xr.Dataset()
    dset2['lon'] = lon
    dset2['lat'] = lat
    dset2['cloud_short'] = cloud_short
    dset2['cloud_long'] = cloud_long
    dset2['cloud_net'] = cloud_net
    dset2 = dset2.assign_coords({'year':np.array([yr])})
    
    return dset2

for yr in range(2003, 2020):
    dset2 = monthly_averages(yr)
    dset2.to_netcdf(# /insert/path/to/save/files/to)
    dset2.close()
    print(yr)