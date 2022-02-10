import os, glob, re, random

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

def process_file(filename):
    # take a single nc profile filename and load it into mongo
    return 0xDEADBEEF

REprefix = re.compile('^[A-Z]*')                 # SD, SR, BD, BR, D or R
REgroup = re.compile('[0-9]*_[0-9]*D{0,1}\.nc')  # everything but the prefix
lograte = 0.001

dacs = os.listdir('/ifremer')

for dac in dacs:
    print('processing DAC: ', dac )
    platforms = os.listdir('/ifremer/'+dac)
    for platform in platforms:
        print('processing platform: ', platform)
        dir = '/ifremer/'+dac+'/'+platform+'/profiles'
        try:
            files = os.listdir(dir)
        except:
            print('warning: /ifremer/'+dac+'/'+platform + ' doesnt have a /profiles dir')
            continue
        profiles = set(map(pickprof, files))
        logsample = random.random()
        for f in files:
            if 'S' in f:
                logsample = logsample*100
                print('enhanced logging')
                break
        for profile in profiles:
            # extract a list of filenames corresponding to this profile, and parse out the set of prefixes to consider
            pfilenames = [ x.split('/')[-1] for x in glob.glob(dir + '/*_' + profile + '.nc')]
            groupname = REgroup.search(pfilenames[0]).group(0)
            prefixes = [REprefix.match(x).group(0) for x in pfilenames]
            # choose by prefix and load data
            selected_prefixes = choose_prefix(prefixes)
            if logsample < lograte:
                print('logging dac / platform / profile: ', dac, platform, profile)
                print('chose prefixes ', selected_prefixes)
            for sp in selected_prefixes:
                process_file(sp + groupname)
