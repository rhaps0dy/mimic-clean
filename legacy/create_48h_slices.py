#!/usr/bin/env python3

###
# Creates slices of 48h for every ventilation
###

import numpy as np
import pandas as pd
import itertools as it
import gzip
import csv
import math

import pickle_utils as pu

def get_headers(table):
    with gzip.open('../mimic-clean/{:s}.csv.gz'.format(table), 'rt',
            newline='') as csvf:
        return next(iter(csv.reader(csvf)))[3:]

def determine_type(header, b_is_category):
    t = header[0]
    if t == 'C':
        return "category"
    elif t == 'B':
        return "category" if b_is_category else np.bool
    elif t == 'F':
        return np.float32
    else:
        raise ValueError(header)

@pu.memoize('48h.pkl.gz')
def get_frequent_headers():
    zero_headers = get_headers('outputevents')
    bool_headers = (get_headers('procedureevents_mv') +
                    get_headers('drugevents'))
    nan_headers = get_headers('labevents') + get_headers('chartevents')

    dtype = dict(it.chain(
        map(lambda e: (e, determine_type(e, True)), nan_headers + zero_headers),
        map(lambda e: (e, determine_type(e, False)), bool_headers),
        map(lambda e: (e, np.int32), ["icustay_id", "hour", "subject_id"])))
    fillna = dict(it.chain(
        map(lambda e: (e, 0.0), zero_headers),
        map(lambda e: (e, False), bool_headers)))

    headers, _ = pu.load("non_missing.pkl.gz")
    headers.sort()
    headers = list(filter(lambda t: dtype[t[1]] != "category",
                          headers))
    _, usecols = zip(*headers[-103:])
    usecols = list(usecols)
    usecols.remove('subject_id')
    usecols.append('B pred last_ventilator')
    del fillna['B pred last_ventilator']

    df = pd.read_csv('../mimic.csv.gz', header=0, index_col='icustay_id',
                usecols=usecols, dtype=dtype, engine='c', true_values=[b'1'],
                false_values=[b'0', b''])
    return df.fillna(fillna)

@pu.memoize('48h_training_examples.pkl.gz')
def get_training_examples(mimic, example_len):
    """ Makes slices for every ventilation-end, with the `example_len` previous
    hours."""
    mimic_select = mimic.select(lambda h: h not in ['hour',
                                'B pred last_ventilator'], axis=1)
    non_nas = len(mimic_select) - mimic_select.isnull().sum()
    mimic_mean = (mimic_select / non_nas).sum()
    examples = []
    for icustay_id, df in mimic.groupby(level=0):
        ventilation_ends = df[['hour', 'B pred last_ventilator']].dropna().values
        if len(ventilation_ends) == 0:
            continue
        last_hour = max(hour for hour, _ in ventilation_ends)
        features = df.select(lambda h: h not in ['hour',
                                    'B pred last_ventilator'], axis=1)
        assert (mimic_select.columns == features.columns).all()
        hours = df['hour']
        val = np.empty(features.shape[1], dtype=np.float32)
        val[:] = np.nan
        t = np.zeros_like(val, dtype=np.int32)
        t += 999 # features initially missing are assumed very stale
        val_next = val.copy()
        t_next = t.copy()
        vent_ts = np.empty((len(ventilation_ends), example_len,
                            features.shape[1]), dtype=np.float32)
        vent_ts[:,:,:] = np.nan
        vent_ts_i = np.zeros(len(ventilation_ends), dtype=np.int32)
        vent_ini_t = [None]*len(ventilation_ends)

        i = 0
        prev_hour = hours.iloc[0]-1000
        while i<len(df) and hours.iloc[i]<=last_hour:
            f = features.iloc[i,:].values.astype(np.float32)
            n = np.isnan(f)
            val_next[-n] = f[-n]
            t_next[-n] = 0
            t_next += n*(hours.iloc[i] - prev_hour)

            for j, (hour, label) in enumerate(ventilation_ends):
                cur_max_i = hours.iloc[i] - hour + example_len
                prev_max_i = prev_hour - hour + example_len
                if 0 < cur_max_i <= example_len:
                    while vent_ts_i[j] < cur_max_i:
                        if vent_ts_i[j] == 0:
                            if cur_max_i == 1:
                                vent_ts[j,0,:] = val_next
                                vent_ini_t[j] = t_next.copy()
                            else:
                                vent_ts[j,0,:] = val
                                vent_ini_t[j] = t + (0-prev_max_i)
                        elif vent_ts_i[j] == cur_max_i-1:
                            vent_ts[j,vent_ts_i[j],:] = f
                        vent_ts_i[j] += 1

                    if cur_max_i == example_len:
                        examples.append((icustay_id, hour, label, vent_ini_t[j], vent_ts[j,:,:]))

            val[:] = val_next
            t[:] = t_next
            prev_hour = hours.iloc[i]
            i += 1
    return examples, mimic_mean

if __name__ == '__main__':
    mimic = get_frequent_headers()
    tex = get_training_examples(mimic, 48)
