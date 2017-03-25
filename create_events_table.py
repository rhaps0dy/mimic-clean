
#!/usr/bin/python

import psycopg2
import sys
import pickle
import re
import itertools as it
from extract_events import ex_float

def merge_dicts(d1, d2):
    for k, v in d2.items():
        if k in d1:
            raise ValueError("Key {:s} belongs to both dictionaries".format(str(k)))
        d1[k] = v
    return d1
