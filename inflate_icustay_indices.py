####
# Create icustay_indices.pkl and (optional) icustay_cluster.pkl
####

import csv
import sys
import itertools as it
import collections
import math
import os.path
import pickle

def interval(icustay_indices, k, hour, **_):
    icustay_indices[k][0] = min(icustay_indices[k][0], hour)
    icustay_indices[k][1] = max(icustay_indices[k][1], hour)
def hour_set(icustay_indices, k, hour, **_):
    icustay_indices[k].add(hour)

def discarded_features(icustay_indices, k, hour, headers, row, cutoffs):
    if hour >= cutoffs[k]:
        return
    for i in range(len(row)):
        if row[i] == '':
            icustay_indices[k].add(headers[i])


def augment_file(fname, fun, default, **kwargs):
    if os.path.isfile(fname):
        with open(fname, 'rb') as f:
            d = pickle.load(f)
    else:
        d = {}
    icustay_indices = collections.defaultdict(default, d)

    for pkl_f in sys.argv[1:]:
        with open(pkl_f, 'r', newline='') as csvfile:
            f = iter(csv.reader(csvfile))
            kwargs['headers'] = next(f)
            for row in f:
                k = (int(row[0]), int(row[1]))
                hour = int(row[2])
                kwargs['row'] = row
                fun(icustay_indices, k, hour, **kwargs)

    with open(fname, 'wb') as f:
        pickle.dump(dict(icustay_indices), f)
    return icustay_indices

def interpret_icustay_cluster(icustay_cluster):
    intervals = {}
    for k, s in icustay_cluster.items():
        ints = []
        s = iter(sorted(list(s)))
        prev_i = start = next(s)
        prev_interval_len = 0
        for i in s:
            if i > prev_i + max(48, prev_interval_len*2):
                ints.append((start, prev_i))
                prev_interval_len = prev_i - start + 1
                start = i
            prev_i = i
        ints.append((start, i))
        intervals[k] = ints
    return intervals

if __name__ == '__main__':
    if len(sys.argv) == 1:
        print("Usage: {:s} <file1.csv> <file2.csv> ...".format(sys.argv[0]))
        sys.exit(1)
    augment_file('icustay_indices.pkl', interval, lambda: [math.inf, -math.inf])
    icustay_cluster = augment_file('icustay_cluster.pkl', hour_set, lambda: set())
