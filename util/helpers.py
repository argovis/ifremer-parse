import re

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
    #     SD always preferred, SR second-preferred
    #     if no synthetic profiles, prefer BD over BR, plus D over R

    if not set(prefixes).issubset(['SD', 'SR', 'BD', 'BR', 'D', 'R']):
        print('error: nonstandard prefix found in list: ' + prefixes)
        return None

    pfx = []
    if 'SD' in prefixes:
        pfx = ['SD']
    elif 'SR' in prefixes:
        pfx = ['SR']
    else:
        if 'BD' in prefixes:
            pfx.append('BD')
        elif 'BR' in prefixes:
            pfx.append('BR')
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
