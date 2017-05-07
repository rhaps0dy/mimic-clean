import os.path
import pickle
import functools
import logging

log = logging.getLogger(__name__)

def memoize_pickle(filename, verbose=True):
    def pk_decorator(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            if os.path.exists(filename):
                if verbose:
                    log.debug("File `{}` exists! Reading data...".format(filename))
                with open(filename, 'rb') as f:
                    data = pickle.load(f)
                return data
            else:
                if verbose:
                    log.debug("Generating data for `{}`...".format(filename))
                data = function(*args, **kwargs)
                with open(filename, 'wb') as f:
                    pickle.dump(data, f)
                return data
        return wrapper
    return pk_decorator

def load_pickle(filename):
    with open(filename, 'rb') as f:
        return pickle.load(f)

def dump_pickle(filename, data):
    with open(filename, 'wb') as f:
        return pickle.dump(f, data)
