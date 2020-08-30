import os
import glob
import xarray as xr
import numpy as np
import pandas as pd


def ice_abs(year):
    """ This function returns an EASE grid with the cumulative radiation absorbed by ice during the melt season.
    Makre sure to replace the paths to app-x 1400 and 0400 passes, the sic and melt mask data-sets and the path 
    where youd like to save the files to.
    """
    
    # fetch 1400 data betwen apr15 and oct15
    os.chdir(path+str(year)) # replace path to app-x 1400 data
    files_1400 = sorted(glob.glob('Polar-APP-X_v02r00_Nhem_1400_d*.nc'))
    files_1400 = files_1400[104:288]
    
    # open 1400 data
    da_1400 = xr.open_mfdataset(files_1400, concat_dim='Time', combine='nested').load()
    albedo_average = da_1400.cdr_surface_albedo.load()
    swd_1400 = da_1400.cdr_surface_downwelling_shortwave_flux.load()
    lwd = da_1400.cdr_surface_downwelling_longwave_flux.load()
    lwu = da_1400.cdr_surface_upwelling_longwave_flux.load()
    #surface_type = da_1400.surface_type.load()
    da_1400.close()
    
    # process 1400 albedo, lwd and lwu to day of year coodinates
    albedo_average.time.data = pd.to_datetime(albedo_average.time.data)
    albedo_average = albedo_average.groupby('time.dayofyear').mean()
    lwd.time.data = pd.to_datetime(lwd.time.data)
    lwd = lwd.groupby('time.dayofyear').mean()
    lwu.time.data = pd.to_datetime(lwu.time.data)
    lwu = lwu.groupby('time.dayofyear').mean()
    
    # fetch 0400 swd data betwen apr15 and oct15
    os.chdir(path+str(year)) # replace path to app-x 0400 data
    files_0400 = sorted(glob.glob('Polar-APP-X_v02r00_Nhem_0400_d*.nc'))
    files_0400 = files_0400[104:288]
    da_0400 = xr.open_mfdataset(files_0400, concat_dim='Time', combine='nested').load()
    swd_0400 = da_0400.cdr_surface_downwelling_shortwave_flux.load()
    da_0400.close()
    
    # find swd average
    swd_0400 = swd_0400.fillna(0)
    swd_0400.time.data = pd.to_datetime(swd_0400.time.data)
    swd_1400.time.data = pd.to_datetime(swd_1400.time.data)
    swd_avg = xr.concat([swd_0400, swd_1400], dim='Time').groupby('time.dayofyear').mean()
    
    # Take out swd outliers that are >3std from rolling mean (seem to be several significant ones)
    r = swd_avg.rolling({'dayofyear':7})  # Create a rolling object (no computation yet)
    swd_avg = r.mean().where(swd_avg < r.mean() + 3 * r.std())
    swd_avg = r.mean().where(swd_avg > r.mean() - 3 * r.std())
    
    # fetch sic data
    file_sic = path+'sic_EASE_'+str(year)+'.nc' # replace path to sic data
    sic_data = xr.open_dataset(file_sic).load()
    sic = sic_data.sic.load()
    sic = sic.where(sic <= 1).where(sic >= 0) # only valid values
    sic_data.close()
    
    # calculate albedo of ice
    albedo_ice = (1/sic) * (albedo_average + (sic-1)*0.06)
    albedo_ice = albedo_ice.where(albedo_ice >= 0).where(albedo_ice <= 1) # only valid values
    
    # fetch melt mask (1 for all cells where ice is melting, ie after melt onset and before freeze up,
    # 0 for before mo and after fup)
    file_melt = path+'cmo_EASE_'+str(year)+'.nc' # replace path to melt mask data
    melt_mask_data = xr.open_dataset(file_melt).load()
    melt_mask = melt_mask_data.melting.load()
    melt_mask_data.close()
    
    # average energy absorbed by ice (weighted by sic) for each day after mo and before fup
    short = melt_mask * sic * ((1 - albedo_ice) * swd_avg)
    long = melt_mask * sic * (lwd - lwu)
    net = melt_mask * sic * ((1-albedo_ice) * swd_avg + lwd - lwu)
        
    # seconds in day
    secs = 86400
    
    # sum joules absorbed
    short = short.sum(dim='dayofyear') * secs
    long = long.sum(dim='dayofyear') * secs
    net = net.sum(dim='dayofyear') * secs
    
    # export data
    ds2 = xr.Dataset()
    ds2['short'] = short
    ds2['long'] = long
    ds2['net'] = net
    ds2 = ds2.assign_coords({'year':np.array([year])})
    
    return ds2

# iterate through the years to calculate
for year in range(1982,2018):
    ds2 = ice_abs(year)
    ds2.to_netcdf(path) # replace with path to save files to
    # saves to raid6 server as the other server cant take it
    ds2.close()
    print('done ', year)

print('finished all')

