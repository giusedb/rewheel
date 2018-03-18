try:
    from cPickle import loads as ploads, dumps as pdumps
except ImportError:
    from pickle import loads as ploads, dumps as pdumps
try:
    from ujson import loads as jloads, dumps as jdumps
except ImportError:
    from json import loads as jloads, dumps as jdumps

