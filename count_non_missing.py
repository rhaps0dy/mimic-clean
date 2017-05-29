#!/usr/bin/env python3

import csv
import gzip
import numpy as np
import pickle_utils as pu

@pu.memoize("non_missing.pkl.gz")
def main():
    with gzip.open('../mimic.csv.gz', 'rt') as gzf:
        f = csv.reader(gzf)
        headers = next(f)
        non_missing = np.zeros(len(headers), dtype=np.int32)
        n_l = 0
        for line in f:
            n_l += 1
            for i, e in enumerate(line):
                if e!='':
                    non_missing[i] += 1
    nm_headers = list(zip(non_missing, headers))
    return nm_headers, n_l 

if __name__ == '__main__':
    main()
