import re, xarray, datetime

def pickprof(filename):
    # identify the profile number from a filename
    # allow a D suffix for decending profiles
    m = re.search('_([0-9]*D{0,1})', filename)
    if m:
        return m.group(1)
    else:
        print('error: failed to find sensible profile number from ' + filename)
        return None

def choose_prefix(prefixes):
    # given a list of nc file prefixes from the set:
    # SD synth delayed
    # SR synth realtime
    # BD bgc delayed
    # BR bgc realtime
    # D  core delayed
    # R  core realtime
    # return which should be chosen as best files to use, based on:
    #     For BGC data: SD always preferred, SR second-preferred, BD and BR always discarded
    #     For core data: D preferred, R second-preferred

    if not set(prefixes).issubset(['SD', 'SR', 'BD', 'BR', 'D', 'R']):
        print('error: nonstandard prefix found in list: ' + prefixes)
        return None

    pfx = []
    # choose synth / BGC data
    if 'SD' in prefixes:
        pfx = ['SD']
    elif 'SR' in prefixes:
        pfx = ['SR']
    # choose core data
    if 'D' in prefixes:
        pfx.append('D')
    elif 'R' in prefixes:
        pfx.append('R')
    return pfx

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

    return key_mapping[nckey]

def pack_objects(measurements):
    # given an object measurements with keys==variable names (temp, temp_qc, pres...) and values equal to depth-ordered lists of the corresponding data,
    # return a depth-ordered list of objects with the appropriate keys.

    ## sanity checks
    if "PRES" not in measurements.keys():
        print('error: measurements objects must have a PRES key.')
        return None
    nLevels = len(measurements['PRES'])
    for var in measurements.keys():
        if len(measurements[var]) != nLevels:
            print('error: measurements', var, 'doesnt have the same number of levels as the provided PRES entry')
            return None
        if var[-3:] != '_QC' and var+'_QC' not in measurements.keys():
            print('error: measurements', var, 'doesnt include a QC vector', var[-3:])

    repack = []
    for i in range(nLevels):
        level = {}
        for key in measurements.keys():
            level[argo_keymapping(key)] = measurements[key][i]
        repack.append(level)

    return repack

def stringcycle(cyclenumber):
    # given a numerical cyclenumber,
    # return a string left padded with 0s appropriate for use in a profile ID

    c = int(cyclenumber)
    if c < 10:
        return '00'+str(c)
    elif c < 100:
        return '0'+str(c)
    else:
        return str(c)

def extract_metadata(ncfile, pidx=0):
    # given the path ncfile to an argo nc file,
    # extract and return a dictionary representing the metadata of the pidx'th profile in that file

    # some helpful facts and figures
    metadata = {}
    xar = xarray.open_dataset(ncfile)
    basins = xarray.open_dataset('parameters/basinmask_01.nc')
    REprefix = re.compile('^[A-Z]*')  
    prefix = REprefix.search(ncfile.split('/')[-1]).group(0)
    variables = list(xar.variables)
    LATITUDE = xar['LATITUDE'].to_dict()['data'][pidx]
    LONGITUDE = xar['LONGITUDE'].to_dict()['data'][pidx]

    ## platform_wmo_number
    metadata['platform_wmo_number'] = int(xar['PLATFORM_NUMBER'].to_dict()['data'][pidx].decode('UTF-8'))

    ## cycle_number
    metadata['cycle_number'] = int(xar['CYCLE_NUMBER'].to_dict()['data'][pidx])

    ## profile_direction
    if('DIRECTION') in variables:
        metadata['profile_direction'] = xar['DIRECTION'].to_dict()['data'][pidx].decode('UTF-8') 

    # id == platform_cycle<D> for primary profile
    metadata['_id'] = str(metadata['platform_wmo_number']) + '_' + stringcycle(metadata['cycle_number'])
    if metadata['profile_direction'] == 'D':
        metadata['_id'] += 'D'

    ## basin
    metadata['basin'] = basins['BASIN_TAG'].sel(LONGITUDE=LONGITUDE, LATITUDE=LATITUDE, method="nearest").to_dict()['data']

    ## data_type
    metadata['data_type'] = 'oceanicProfile'

    ## doi: TODO

    ## geolocation
    metadata['geolocation'] = {"type": "Point", "coordinates": [LONGITUDE, LATITUDE]}

    ## instrument
    metadata['instrument'] = 'profiling_float'

    ## source TODO: what about argo_deep?
    if prefix in ['R', 'D']:
        metadata['source'] = ['argo_core']
    elif prefix in ['SR', 'SD']:
        metadata['source'] = ['argo_bgc'] # TODO check if this is the intended interpretation

    ## data_center
    if('DATA_CENTRE') in variables:
      metadata['data_center'] = xar['DATA_CENTRE'].to_dict()['data'][pidx].decode('UTF-8')

    ## source_url
    metadata['source_url'] = 'ftp://ftp.ifremer.fr/ifremer/argo/dac/' + ncfile[9:]

    ## timestamp: TODO make sure this doesn't get mangled on insertion
    metadata['timestamp'] = xar['JULD'].to_dict()['data'][pidx]

    ## date_updated_argovis
    metadata['date_updated_argovis'] = datetime.datetime.now()

    ## date_updated_source
    metadata['date_updated_source'] = datetime.datetime.strptime(xar['DATE_UPDATE'].to_dict()['data'].decode('UTF-8'),'%Y%m%d%H%M%S')

    ## pi_name
    if('PI_NAME') in variables:
        metadata['pi_name'] = xar['PI_NAME'].to_dict()['data'][pidx].decode('UTF-8').strip()

    ## country: TODO

    ## geolocation_argoqc
    if('POSITION_QC') in variables:
        metadata['geolocation_argoqc'] = int(xar['POSITION_QC'].to_dict()['data'][pidx].decode('UTF-8'))

    ## timestamp_argoqc
    if('JULD_QC') in variables:
        metadata['timestamp_argoqc'] = int(xar['JULD_QC'].to_dict()['data'][pidx].decode('UTF-8'))

    ## fleetmonitoring
    metadata['fleetmonitoring'] = 'https://fleetmonitoring.euro-argo.eu/float/' + str(metadata['platform_wmo_number'])

    ## oceanops
    metadata['oceanops'] = 'https://www.ocean-ops.org/board/wa/Platform?ref=' + str(metadata['platform_wmo_number'])

    ## platform_type
    if('PLATFORM_TYPE') in variables:
      metadata['platform_type'] = xar['PLATFORM_TYPE'].to_dict()['data'][pidx].decode('UTF-8').strip()

    ## positioning_system
    if('POSITIONING_SYSTEM') in variables:
      metadata['positioning_system'] = xar['POSITIONING_SYSTEM'].to_dict()['data'][pidx].decode('UTF-8').strip()

    ## vertical_sampling_scheme
    if('VERTICAL_SAMPLING_SCHEME') in variables:
      metadata['vertical_sampling_scheme'] = xar['VERTICAL_SAMPLING_SCHEME'].to_dict()['data'][pidx].decode('UTF-8').strip()

    ## wmo_inst_type
    if('WMO_INST_TYPE') in variables:
      metadata['wmo_inst_type'] = xar['WMO_INST_TYPE'].to_dict()['data'][pidx].decode('UTF-8').strip()

    xar.close()
    basins.close()
    return metadata

def compare_metadata(metadata):
    # given a list of metadata objects as returned by extract_metadata,
    # return true if all list elements are mutually consistent with having come from the same profile

    comparisons = ['platform_wmo_number', 'cycle_number', '_id', 'basin', 'data_type', 'geolocation', 'instrument', 'data_center', 'timestamp', 'pi_name', 'geolocation_argoqc', 'timestamp_argoqc', 'fleetmonitoring', 'oceanops', 'platform_type', 'positioning_system', 'vertical_sampling_scheme', 'wmo_inst_type']

    for m in metadata[1:]:
        for c in comparisons:
            if c in metadata[0] and c in m:
                if metadata[0][c] != m[c]:
                    print(metadata[0][c], m[c])
                    return False
                elif (c in metadata[0] and c not in m) or (c not in metadata[0] and c in m):
                    return False

    return True

def extract_data(xzr, pidx=0):
    # given an xarray object representing an Argo nc file,
    # extract and return a dictionary representing the data of the pidx'th profile in that file

    data = {}

    return data