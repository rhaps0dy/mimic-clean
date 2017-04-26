import csv
import collections

subj_vent = collections.defaultdict(lambda: 0, {})
icu_vent = collections.defaultdict(lambda: 0, {})
subj_revent = collections.defaultdict(lambda: 0, {})
icu_revent = collections.defaultdict(lambda: 0, {})

with open('drugevents.csv') as csvfile:
    f = csv.reader(csvfile)
    next(f)
    for row in f:
        sid = int(row[0])
        icu = int(row[1])
        invent = row[5]
        revent = row[3]
        if invent == '1':
            subj_vent[sid] = 1
            icu_vent[icu] = 1
        if revent == '0':
            subj_revent[sid] += 1
            icu_revent[sid] += 1
