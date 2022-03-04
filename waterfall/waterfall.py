from pymongo import MongoClient
from geopy import distance
from itertools import compress
import wget, fsspec, xarray, time, re, datetime, math

def find_basin(lon, lat):
    # for a given lon, lat,
    # identify the basin from the lookup table.
    # choose the nearest non-nan grid point.

    gridspacing = 0.5
    basins = xarray.open_dataset('parameters/basinmask_01.nc')

    basin = basins['BASIN_TAG'].sel(LONGITUDE=lon, LATITUDE=lat, method="nearest").to_dict()['data']
    if math.isnan(basin):
        # nearest point was on land - find the nearest non nan instead.
        lonplus = math.ceil(lon / gridspacing)*gridspacing
        lonminus = math.floor(lon / gridspacing)*gridspacing
        latplus = math.ceil(lat / gridspacing)*gridspacing
        latminus = math.floor(lat / gridspacing)*gridspacing
        grids = [(basins['BASIN_TAG'].sel(LONGITUDE=lonminus, LATITUDE=latminus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latminus, lonminus)).miles),
                 (basins['BASIN_TAG'].sel(LONGITUDE=lonminus, LATITUDE=latplus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latplus, lonminus)).miles),
                 (basins['BASIN_TAG'].sel(LONGITUDE=lonplus, LATITUDE=latplus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latplus, lonplus)).miles),
                 (basins['BASIN_TAG'].sel(LONGITUDE=lonplus, LATITUDE=latminus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latminus, lonplus)).miles)]

        grids = [x for x in grids if not math.isnan(x[0])]
        if len(grids) == 0:
            # all points on land
            #print('warning: all surrounding basin grid points are NaN')
            basin = -1
        else:
            grids.sort(key=lambda tup: tup[1])
            basin = grids[0][0]
    basins.close()
    return int(basin)

def argo_keymapping(nckey):
    # argo netcdf measurement name -> argovis measurement name

    key_mapping = {
        "PRES": "pres",
        "TEMP": "temp",
        "PSAL": "psal",
        "CNDX": "cndx",
        "DOXY": "doxy",
        "CHLA": "chla",
        "CDOM": "cdom",
        "NITRATE": "nitrate",
        "BBP700": "bbp700",
        "DOWN_IRRADIANCE412": "down_irradiance412",
        "DOWN_IRRADIANCE442": "down_irradiance442",
        "DOWN_IRRADIANCE490": "down_irradiance490",
        "DOWNWELLING_PAR": "downwelling_par",
        "PRES_QC": "pres_qc",
        "TEMP_QC": "temp_qc",
        "PSAL_QC": "psal_qc",
        "CNDX_QC": "cndx_qc",
        "DOXY_QC": "doxy_qc",
        "CHLA_QC": "chla_qc",
        "CDOM_QC": "cdom_qc",
        "NITRATE_QC": "nitrate_qc",
        "BBP700_QC": "bbp700_qc",
        "DOWN_IRRADIANCE412_QC": "down_irradiance412_qc",
        "DOWN_IRRADIANCE442_QC": "down_irradiance442_qc",
        "DOWN_IRRADIANCE490_QC": "down_irradiance490_qc",
        "DOWNWELLING_PAR_QC": "downwelling_par_qc"
    }

    try:
        argoname = key_mapping[nckey.replace('_ADJUSTED', '')]
    except:
        print('warning: unexpected variable found in station_parameters:', nckey)
        argoname = nckey.replace('_ADJUSTED', '').lower()

    return argoname

def cleanup(meas):
    # given a measurement, return the measurement after some generic cleanup

    if meas is None:
        return meas

    # qc codes come in as bytes, should be ints
    try:
        ducktype = meas.decode('UTF-8')
        return int(meas)
    except:
        pass

    # use None as missing fill
    if math.isnan(meas):
        return None        

    return round(meas,6) # at most 6 significant decimal places

client = MongoClient('mongodb://database/argo')
db = client.argo

while True:
	time.sleep(60)
	#p = list(db.profilesx.aggregate([{"$sample": {"size": 1}}]))[0]
	p = db.profilesx.find_one({"_id":"5900476_000"})
	nc = []
	for source in p['source_info']:
		fobj = fsspec.open(source['source_url']).open()
		nc.append({
			"source": source['source_url'],
			"fobj": fobj,
			"data": xarray.open_dataset(fobj)
		})

	# check data integrity mongo <--> ifremer
	for xar in nc:
		if p['platform_wmo_number'] != int(xar['data']['PLATFORM_NUMBER'].to_dict()['data'][0].decode('UTF-8')):
			print('platform_wmo_number mismatch at', xar['source'])

		if p['cycle_number'] != int(xar['data']['CYCLE_NUMBER'].to_dict()['data'][0]):
			print('cycle_number mismatch at', xar['source'])

		if 'DIRECTION' in list(xar['data'].variables):
			if p['profile_direction'] != xar['data']['DIRECTION'].to_dict()['data'][0].decode('UTF-8'):
				print('profile_direction mismatch at', xar['source'])

		reconstruct_id = str(p['platform_wmo_number']) + '_' + str(p['cycle_number']).zfill(3)		
		if 'profile_direction' in p and p['profile_direction'] == 'D':
			reconstruct_id += str(p['profile_direction']) 
		if p['_id'] != reconstruct_id:
			print('profile _id mangled for', xar['source'], p['_id'], reconstruct_id)

		if p['basin'] != find_basin(xar['data']['LONGITUDE'].to_dict()['data'][0], xar['data']['LATITUDE'].to_dict()['data'][0]):
			print('basin mismatch at', xar['source'])

		if p['data_type'] != 'oceanicProfile':
			print('data_type mismatch at', xar['source'])

		if p['geolocation'] != {'type': 'Point', 'coordinates': [xar['data']['LONGITUDE'].to_dict()['data'][0], xar['data']['LATITUDE'].to_dict()['data'][0]]}:
			print('geolocation mismatch at', xar['source'])

		if p['instrument'] != 'profiling_float':
			print('instrument mismatch at', xar['source'])

		si = {}
		REprefix = re.compile('^[A-Z]*')  
		prefix = REprefix.search(xar['source'].split('/')[-1]).group(0)
		if prefix in ['R', 'D']:
			si['source'] = ['argo_core']
		elif prefix in ['SR', 'SD']:
			si['source'] = ['argo_bgc']
		si['source_url'] = xar['source']
		si['date_updated_source'] = datetime.datetime.strptime(xar['data']['DATE_UPDATE'].to_dict()['data'].decode('UTF-8'),'%Y%m%d%H%M%S')
		si['data_keys_source'] = [key.decode('UTF-8').strip() for key in xar['data']['STATION_PARAMETERS'].to_dict()['data'][0]]
		if si not in p['source_info']:
			print('source_info mismatch at', xar['source'])

		if p['data_center'] != xar['data']['DATA_CENTRE'].to_dict()['data'][0].decode('UTF-8'):
			print('data_center mismatch at', xar['source'])

		td = p['timestamp'] - xar['data']['JULD'].to_dict()['data'][0]
		if not datetime.timedelta(milliseconds=-1) <= td <= datetime.timedelta(milliseconds=1):
			print('timestamp mismatch at', xar['source'])

		if 'date_updated_argovis' not in p:
			print('date_updated_argovis absent from profile derived from', xar['source'])

		if 'PI_NAME' in list(xar['data'].variables):
			if p['pi_name'] != xar['data']['PI_NAME'].to_dict()['data'][0].decode('UTF-8').strip().split(','):
				print('pi_name mismatch at', xar['source'])

		if 'POSITION_QC' in list(xar['data'].variables):
			if p['geolocation_argoqc'] != int(xar['data']['POSITION_QC'].to_dict()['data'][0].decode('UTF-8')):
				print('geolocation_argoqc mismatch at', xar['source'])

		if 'JULD_QC' in list(xar['data'].variables):
			if p['timestamp_argoqc'] != int(xar['data']['JULD_QC'].to_dict()['data'][0].decode('UTF-8')):
				print('timestamp_argoqc mismatch at', xar['source'])

		if p['fleetmonitoring'] != 'https://fleetmonitoring.euro-argo.eu/float/' + str(p['platform_wmo_number']):
			print('fleetmonitoring mismatch at', xar['source'])

		if p['oceanops'] != 'https://www.ocean-ops.org/board/wa/Platform?ref=' + str(p['platform_wmo_number']):
			print('oceanops mismatch at', xar['source'])

		if 'PLATFORM_TYPE' in list(xar['data'].variables):
			if p['platform_type'] != xar['data']['PLATFORM_TYPE'].to_dict()['data'][0].decode('UTF-8').strip():
				print('platform_type mismatch at', xar['source'])

		if 'POSITIONING_SYSTEM' in list(xar['data'].variables):
			if p['positioning_system'] != xar['data']['POSITIONING_SYSTEM'].to_dict()['data'][0].decode('UTF-8').strip():
				print('positioning_system mismatch at', xar['source'])

		if 'VERTICAL_SAMPLING_SCHEME' in list(xar['data'].variables):
			if p['vertical_sampling_scheme'] != xar['data']['VERTICAL_SAMPLING_SCHEME'].to_dict()['data'][0].decode('UTF-8').strip():
				print('vertical_sampling_scheme mismatch at', xar['source'])

		if 'WMO_INST_TYPE' in list(xar['data'].variables):
			if p['wmo_inst_type'] != xar['data']['WMO_INST_TYPE'].to_dict()['data'][0].decode('UTF-8').strip():
				print('wmo_inst_type mismatch at', xar['source'])

		if prefix in ['R', 'D']:
			# check core data
			DATA_MODE = xar['data']['DATA_MODE'].to_dict()['data'][0].decode('UTF-8')
			if DATA_MODE in ['A', 'D']:
				# check adjusted data
				data_sought = [f(x) for x in xar['data']['STATION_PARAMETERS'].to_dict()['data'][0] for f in (lambda name: name.decode('UTF-8').strip()+'_ADJUSTED',lambda name: name.decode('UTF-8').strip()+'_ADJUSTED_QC')]
				data_sought.remove('PRES_ADJUSTED') # will check pressure implicitly when looking up other variables
				nc_pressure = xar['data']['PRES_ADJUSTED'].to_dict()['data'][0]
			elif DATA_MODE == 'R':
				data_sought = [f(x) for x in xar['data']['STATION_PARAMETERS'].to_dict()['data'][0] for f in (lambda name: name.decode('UTF-8').strip(),lambda name: name.decode('UTF-8').strip()+'_QC')]
				data_sought.remove('PRES')
				nc_pressure = xar['data']['PRES'].to_dict()['data'][0]
			else:
				print('unrecognized DATA_MODE for', xar['source'])

			p_data = list(zip(*p['data']))
			nc_pressure = [round(x,6) for x in nc_pressure] # these appear to not actually be ordered! see 5900476_000
			nc_pressure_sorted = sorted(nc_pressure)
			p_pressure = p_data[p['data_keys'].index('pres')]
			pressure_mask = [x in nc_pressure_sorted for x in p_pressure]
			print('upstream pressure:', nc_pressure)
			print('sorted upstream pressure', nc_pressure_sorted)
			print('argovis pressure:', p_pressure)
			print('pressure mask:', pressure_mask)
			for var in data_sought:
				argo_var = argo_keymapping(var)
				p_values = p_data[p['data_keys'].index(argo_var)]
				masked_p_vals = list(compress(p_values, pressure_mask)) # should be just the variable values corresponding to pressures found in this file
				nc_values = xar['data'][var].to_dict()['data'][0]
				nc_values = [cleanup(x) for x in nc_values]
				nc_values_sorted = list(zip(nc_pressure, nc_values)) # zip together with pressure, sort by pressure, and unzip
				nc_values_sorted.sort(key=lambda tup: tup[0])
				nc_values_sorted = [x[1] for x in nc_values_sorted] 
				print(var, 'upstream:', nc_values)
				print(var, 'upstream sorted:', nc_values_sorted)
				print(var, 'unmasked:', p_values)
				print(var, 'masked:', masked_p_vals)
				
				if masked_p_vals != nc_values_sorted:
					print('data mismatch at', var, 'in', xar['source'])

		print('finished', xar['source'])


	for source in nc:
		source['fobj'].close()
