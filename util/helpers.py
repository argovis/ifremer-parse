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