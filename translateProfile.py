# usage: python translateProfile.py <absolute paths to all Argo netcdf files relevant for this profile>
# populates mongodb argo/profilesx with the contents of the provided file
# assumes establishCollection.py was run first to create the collection with schema enforcement
# assumes rsync populated content under /ifremer, same as choosefiles.py

import sys, pymongo, xarray, re, datetime, difflib, pprint, numpy
import util.helpers as h

print('parsing', sys.argv[1:])

# input data and parameters
#data = [xarray.open_dataset(x) for x in sys.argv[1:]]
# print(data['PRES_ADJUSTED'].to_dict()['data'])
# print(data['PRES_ADJUSTED_QC'].to_dict()['data'])
# print(data['TEMP_ADJUSTED'].to_dict()['data'])
# print(data['TEMP_ADJUSTED_QC'].to_dict()['data'])
# print(data['PSAL_ADJUSTED'].to_dict()['data'])
# print(data['PSAL_ADJUSTED_QC'].to_dict()['data'])
# print(data['DOXY_ADJUSTED'].to_dict()['data'])
# print(data['DOXY_ADJUSTED_QC'].to_dict()['data'])


# # helpful constants
# REid = re.compile('[0-9]*_[0-9]*D{0,1}')	# platform_cycle<D>
# REprefix = re.compile('^[A-Z]*')   
# ## TODO: review after all keys are implemented
# mandatory_keys = ['LATITUDE', 'LONGITUDE', 'DATA_TYPE', 'JULD', 'DATE_UPDATE', 'CYCLE_NUMBER', 'PLATFORM_NUMBER']
# optional_keys = [ 'PI_NAME', 'DATA_CENTRE', 'DIRECTION', 'POSITION_QC', 'JULD_QC', 'PLATFORM_TYPE', 'POSITIONING_SYSTEM', 'VERTICAL_SAMPLING_SCHEME', 'WMO_INST_TYPE']
# measurement_keys = ["PRES","TEMP","PSAL","CNDX","DOXY","CHLA","CDOM","NITRATE","BBP700","DOWN_IRRADIANCE412","DOWN_IRRADIANCE442","DOWN_IRRADIANCE490","DOWNWELLING_PAR"]
# n_prof = data.dims['N_PROF']
# prefix = REprefix.search(sys.argv[1].split('/')[-1]).group(0)
# if prefix not in ['SD', 'SR', 'BD', 'BR', 'D', 'R']:
# 	print('error: found unsupported file prefix', prefix)
# 	sys.exit()

#----
# look for mandatory and optional keys, complain appropriately

# extract metadata for each file and make sure it's consistent between all files being merged
profiles = [h.extract_metadata(x) for x in sys.argv[1:]]
if not h.compare_metadata(profiles):
	print('error: files', sys.argv[1:], 'did not yield consistent metadata')

# extract data variables for each file
#data = [h.extract_data(x) for x in data]

# merge metadata into single object

# merge data into single pressure axis list

# combine metadata + data and return

#----

# for i in range(n_prof):
# 	p = {}

# 	# look for mandatory and optional keys, complain appropriately
# 	datakeys = set(data.keys())
# 	missing_mandatory = set(mandatory_keys) - datakeys
# 	if len(missing_mandatory) > 0:
# 		print('error: profile index', i, 'is missing required keys', missing_mandatory)
# 		break
# 	missing_optional = set(optional_keys) - datakeys
# 	for key in missing_optional:
# 		# attempt to detect misspellings
# 		spellingerror = difflib.get_close_matches(key, list(datakeys), cutoff=0.7)
# 		if len(spellingerror) > 0:
# 			print('warning: key', key, 'not found, but did find similar key', spellingerror[0])

# 	# extract some generically helpful facts
# 	LATITUDE = data['LATITUDE'].to_dict()['data'][i]
# 	LONGITUDE = data['LONGITUDE'].to_dict()['data'][i]

# 	# set schema keys

# 	## id == platform_cycle<D> for primary profile; append _i for extra profiles
# 	p['_id'] = REid.search(sys.argv[1].split('/')[-1]).group(0)
# 	if i>0:
# 		p['_id'] = p['_id'] + '_' + str(i)

# 	## basin TODO: currently best guess on how to interpret lookup file, careful validation required
# 	p['basin'] = basins['BASIN_TAG'].sel(LONGITUDE=LONGITUDE, LATITUDE=LATITUDE, method="nearest").to_dict()['data']

# 	## data_type TODO: validate desired allowed values, create mapping.
# 	p['data_type'] = data['DATA_TYPE'].to_dict()['data'].decode('UTF-8').strip()

# 	## doi: TODO

# 	## geolocation
# 	p['geolocation'] = {"type": "Point", "coordinates": [LONGITUDE, LATITUDE]}

#     ## instrument TODO: check that this was what's intended forArgo
# 	p['instrument'] = 'profiling_float'

#     ## data: TODO significant validation required
# 	measurements = {}
# 	for m in measurement_keys:
# 		# pass along the adjusted values where available
# 		# value and value_qc should both be present and both be adjusted or both not, mixing shouldn't be possible
# 		if m+'_ADJUSTED' in list(data.variables) and m+'_ADJUSTED_QC' in list(data.variables):
# 			measurements[m] = data[m+'_ADJUSTED'].to_dict()['data'][i]
# 			measurements[m+'_QC'] = [float(x) for x in data[m+'_ADJUSTED_QC'].to_dict()['data'][i]]
# 		elif m in list(data.variables) and m+'_QC' in list(data.variables):
# 			measurements[m] = data[m].to_dict()['data'][i]
# 			measurements[m+'_QC'] = [float(x) for x in data[m+'_QC'].to_dict()['data'][i]]
# 		elif not set([m, m+'_QC', m+'_ADJUSTED', m+'_ADJUSTED_QC']).isdisjoint(list(data.variables)):
# 			print('error: measurement', m, 'or its QC found in nc file without matching QC or measurement.')
# 	p['data'] = h.pack_objects(measurements)

#     ## data_keys: TODO computed independently of p['data']; tie dependence, or use as crosscheck?
# 	p['data_keys'] = []
# 	for meas in measurement_keys:
# 		if (meas in list(data.variables) and numpy.any(data[meas].to_dict()['data'][i])) or (meas+'_ADJUSTED' in list(data.variables) and numpy.any(data[meas+'_ADJUSTED'].to_dict()['data'][i])):
# 			p['data_keys'].append(h.argo_keymapping(meas))

#     ## data_keys_source: TODO

#     ## source TODO: argo_deep?
# 	if prefix in ['R', 'D']:
# 		p['source'] = ['argo_core']
# 	elif prefix in ['BR', 'BD']:
# 		p['source'] = ['argo_bgc']
# 	elif prefix in ['SR', 'SD']:
# 		p['source'] = ['argo_bgc', 'argo_core'] # TODO check if this is the intended interpretation

#     ## source_url
# 	p['source_url'] = 'ftp://ftp.ifremer.fr/ifremer/argo/dac/' + sys.argv[1][9:]

#     ## timestamp: TODO make sure this doesn't get mangled on insertion
# 	p['timestamp'] = data['JULD'].to_dict()['data'][i]

#     ## date_updated_argovis
# 	p['date_updated_argovis'] = datetime.datetime.now()

# 	## date_updated_source
# 	p['date_updated_source'] = datetime.datetime.strptime(data['DATE_UPDATE'].to_dict()['data'].decode('UTF-8'),'%Y%m%d%H%M%S')

# 	## pi_name
# 	if('PI_NAME') in datakeys:
# 		p['pi_name'] = [x.decode('UTF-8').strip() for x in data['PI_NAME'].to_dict()['data']]

# 	## country: TODO

# 	## data_center
# 	if('DATA_CENTRE') in datakeys:
# 		p['data_center'] = data['DATA_CENTRE'].to_dict()['data'][i].decode('UTF-8')

# 	## profile_direction
# 	if('DIRECTION') in datakeys:
# 		p['profile_direction'] = data['DIRECTION'].to_dict()['data'][i].decode('UTF-8')	

# 	## geolocation_argoqc
# 	if('POSITION_QC') in datakeys:
# 		p['geolocation_argoqc'] = int(data['POSITION_QC'].to_dict()['data'][i].decode('UTF-8'))

# 	## timestamp_argoqc
# 	if('JULD_QC') in datakeys:
# 		p['timestamp_argoqc'] = int(data['JULD_QC'].to_dict()['data'][i].decode('UTF-8'))

# 	## cycle_number
# 	p['cycle_number'] = data['CYCLE_NUMBER'].to_dict()['data'][i]

# 	## data_key_mode: TODO, only applies to BGC via PARAMETERT_DATA_MODE

# 	## platform_wmo_number
# 	p['platform_wmo_number'] = int(data['PLATFORM_NUMBER'].to_dict()['data'][i].decode('UTF-8'))

# 	## fleetmonitoring TODO: drop? redundant with platform_wmo_number
# 	p['fleetmonitoring'] = 'https://fleetmonitoring.euro-argo.eu/float/' + str(p['platform_wmo_number'])

#     ## oceanops TODO: drop? redundant with platform_wmo_number
# 	p['oceanops'] = 'https://www.ocean-ops.org/board/wa/Platform?ref=' + str(p['platform_wmo_number'])

# 	## platform_type
# 	if('PLATFORM_TYPE') in datakeys:
# 		p['platform_type'] = data['PLATFORM_TYPE'].to_dict()['data'][i].decode('UTF-8').strip()

# 	## positioning_system
# 	if('POSITIONING_SYSTEM') in datakeys:
# 		p['positioning_system'] = data['POSITIONING_SYSTEM'].to_dict()['data'][i].decode('UTF-8').strip()

# 	## vertical_sampling_scheme
# 	if('VERTICAL_SAMPLING_SCHEME') in datakeys:
# 		p['vertical_sampling_scheme'] = data['VERTICAL_SAMPLING_SCHEME'].to_dict()['data'][i].decode('UTF-8').strip()

# 	## wmo_inst_type
# 	if('WMO_INST_TYPE') in datakeys:
# 		p['wmo_inst_type'] = data['WMO_INST_TYPE'].to_dict()['data'][i].decode('UTF-8').strip()

# 	profiles.append(p)

# pprint.pprint(profiles)