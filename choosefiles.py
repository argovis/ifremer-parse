import os, glob, re, random
import util.helpers as h

def process_file(filename):
    # take a single nc profile filename and load it into mongo
    return 0xDEADBEEF

REprefix = re.compile('^[A-Z]*')                 # SD, SR, BD, BR, D or R
REgroup = re.compile('[0-9]*_[0-9]*D{0,1}\.nc')  # everything but the prefix

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
        profiles = set(map(h.pickprof, files))
        for profile in profiles:
            # extract a list of filenames corresponding to this profile, and parse out the set of prefixes to consider
            pfilenames = [ x.split('/')[-1] for x in glob.glob(dir + '/*_' + profile + '.nc')]
            groupname = REgroup.search(pfilenames[0]).group(0)
            prefixes = [REprefix.match(x).group(0) for x in pfilenames]
            # choose by prefix and load data
            selected_prefixes = h.choose_prefix(prefixes)
            for sp in selected_prefixes:
                process_file(sp + groupname)
