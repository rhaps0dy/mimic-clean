import csv
import sys
import itertools as it
import collections
import math
import pickle

def get_sets(fname):
    patients = collections.defaultdict(lambda: [math.inf, -math.inf], {})
    patient_hours = set()
    n_rows = 0
    with open(fname, 'r', newline='') as csvfile:
        f = csv.reader(csvfile)
        f = iter(f)
        next(f)
        for row in f:
            k = (int(row[0]), int(row[1]))
            hour = int(row[2])
            patients[k][0] = min(patients[k][0], hour)
            patients[k][1] = max(patients[k][1], hour)
            patient_hours.add(k + (hour,))
            n_rows += 1
    return patients, patient_hours, n_rows

if __name__ == '__main__':
    p1, ph1, nr1 = get_sets(sys.argv[1])
    p2, ph2, nr2 = get_sets(sys.argv[2])
    print("{:s}: {:d} icustays, {:d} icustay-hours, {:d} rows".format(sys.argv[1], len(p1), len(ph1), nr1))
    print("{:s}: {:d} icustays, {:d} icustay-hours, {:d} rows".format(sys.argv[2], len(p2), len(ph2), nr2))

    for p, t in p2.items():
        p1[p][0] = min(p1[p][0], t[0])
        p1[p][1] = max(p1[p][1], t[1])
    with open('icustay_indices.pkl', 'wb') as f:
        pickle.dump(dict(p1), f)
