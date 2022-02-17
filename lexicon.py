# usage: python lexicon.py
# assumes rsync populated content under /ifremer

import os, xarray

dacs = os.listdir('/ifremer')
lex = open("/tmp/lexicon.txt", "w")

varlex = set([])
datatypelex = set([])

for dac in dacs:
    platforms = os.listdir('/ifremer/'+dac)
    for platform in platforms:
        folder = '/ifremer/'+dac+'/'+platform+'/profiles'
        try:
            files = os.listdir(folder)
        except:
            continue
        for file in files:
            # data = xarray.open_dataset(folder + '/' + file)
            # varlex.update(set(data.variables))
            # datatypelex.update(set(data['DATA_TYPE'].to_dict()['data']))
            # data.close()
            with xarray.open_dataset(folder + '/' + file, cache=False) as ds:
                varlex.update(set(ds.variables))
                datatypelex.update(set(ds['DATA_TYPE'].to_dict()['data']))

lex.write(str(varlex))
lex.write('\n\n\n\n\n')
lex.write(str(datatypelex))

lex.close()
