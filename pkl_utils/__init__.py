import os.path
import pickle
import functools
import logging
import gzip

__all__ = ['load_pickle', 'dump_pickle', 'memoize_pickle']
log = logging.getLogger(__name__)

def _smart_open(filename, *args, **kwargs):
    if filename[-7:] == '.pkl.gz':
        return gzip.open(filename, *args, **kwargs)
    elif filename[-4:] == '.pkl':
        return open(filename, *args, **kwargs)
    else:
        raise ValueError("Unknown extension in: `{:s}`".format(filename))

def load_pickle(filename):
    with _smart_open(filename, 'rb') as f:
        return pickle.load(f)

def dump_pickle(data, filename):
    with _smart_open(filename, 'wb') as f:
        return pickle.dump(data, f)

def memoize_pickle(filename, verbose=True):
    def pk_decorator(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            if os.path.exists(filename):
                if verbose:
                    log.debug("File `{}` exists! Reading data...".format(filename))
                data = load_pickle(filename)
                return data
            else:
                if verbose:
                    log.debug("Generating data for `{}`...".format(filename))
                data = function(*args, **kwargs)
                dump_pickle(data, filename)
                return data
        return wrapper
    return pk_decorator
