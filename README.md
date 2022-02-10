WIP: given a set of argo .nc files as rsync'ed by something like https://github.com/argovis/ifremer-sync, traverse the files, decide which ones to load into argovis, and do so.

Exepectations:

 - rsync has populated a directory with a standard mirror of profile content, and that directory is mounted at `/ifremer` inside this container.
