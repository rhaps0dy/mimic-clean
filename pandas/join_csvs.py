import numpy as np
import pandas as pd
import os.path
import collections

def calculate_cutoffs(n_buckets):
    import csv
    import collections
    import pickle
    if not os.path.isfile('n_items.pkl'):
        with open('../chartevents.csv') as csvfile:
            i = csv.reader(csvfile)
            next(i)
            c = collections.Counter(map(lambda k: k[0], i))
        a = np.zeros(max(map(int, c.keys()))+1)
        for i, count in c.items():
            a[int(i)] = count
        with open('n_items.pkl', 'wb') as f:
            pickle.dump(a, f)
    else:
        with open('n_items.pkl', 'rb') as f:
            a = pickle.load(f)
    bucket_sz = np.sum(a) / n_buckets
    i = 0
    cutoffs = []
    print(a.shape)
    while i < a.shape[0]:
        s = 0
        while s < bucket_sz and i < a.shape[0]:
            s += a[i]
            i += 1
        cutoffs.append(i)
    return cutoffs[:-1]

def bsearch(df, v):
    low = 0
    high = df.shape[0]
    while low < high:
        i = (low+high)//2
        if df.index[i][0] < v:
            low = i+1
        else:
            high = i
    return low

CUTOFFS = calculate_cutoffs(8192*4)

def k_table_cutoff(table, cutoff):
    return "{:s}/sid_{:05d}".format(table, cutoff)

def cutoff_csv_to_hdf(hdf, csv_path):
    csv = pd.read_csv(csv_path, header=0, index_col=[0,1,2], chunksize=40000)
    table = os.path.basename(csv_path).split('.')[0]
    cutoff_i = 0
    acc = None
    total_len = 0
    cut_j = 0
    for chunk in csv:
        print("One more chunk", chunk.index[-1][0])
        total_len += chunk.shape[0]
        if acc is None:
            acc = chunk
        else:
            if not acc.index[-1][0] <= chunk.index[0][0]:
                import pdb
                pdb.set_trace()
            acc = pd.concat([acc,chunk])

        while cutoff_i < len(CUTOFFS):
            while cut_j < acc.shape[0] and acc.index[cut_j][0] < CUTOFFS[cutoff_i]:
                if cut_j > 0:
                    assert acc.index[cut_j][0] >= acc.index[cut_j-1][0], \
                            "Data should be sorted by subject_id"
                cut_j += 1
            if cut_j >= acc.shape[0]:
                break

            if cutoff_i == 0:
                ci = 0
            else:
                ci = CUTOFFS[cutoff_i-1]
            hdf.put(k_table_cutoff(table, ci), acc.head(cut_j))
            acc_ = acc.tail(acc.shape[0]-cut_j)
            del acc
            acc = acc_
            cut_j = 0
            cutoff_i += 1
    hdf.put(k_table_cutoff(table, CUTOFFS[-1]), acc)
    return total_len

def verify_record_length(hdf, table, total_len, empty_df):
    s = 0
    for c in [0] + CUTOFFS:
        try:
            s += hdf.get(k_table_cutoff(table, c)).shape[0]
        except KeyError:
            hdf.put(k_table_cutoff(table, c), empty_df)
    assert total_len == s, "We forgot some record"

class HourIterator:
    def __init__(self, hour_limits):
        self.hour_limits = hour_limits
        self.keys = sorted(list(hour_limits.keys()))
        self.len = sum(end-start+1 for start, end in
                map(self.hour_limits.__getitem__, self.keys))

    def __len__(self):
        return self.len

    def __iter__(self):
        self.i_keys = iter(self.keys)
        self.cur_key = ()
        self.cur_iter = iter([])
        return self

    def __next__(self):
        try:
            return self.cur_key + (next(self.cur_iter),)
        except StopIteration:
            self.cur_key = next(self.i_keys)
            start, end = self.hour_limits[self.cur_key]
            self.cur_iter = iter(range(start, end+1))
            return self.cur_key + (next(self.cur_iter),)

tables = ['chartevents', 'outputevents', 'drugevents', 'procedureevents_mv', 'labevents']
def create_hdf():
    total_len = [2092132, 1267804, 1102015, 2004837, 664453]
    for i, t in enumerate(tables):
        print("Doing table", t)
        with pd.HDFStore('mimic.h5') as hdf:
            total_len[i] = cutoff_csv_to_hdf(hdf, '../{:s}.csv'.format(t))

    print("Verifying the tables...")
    empty_idx = pd.MultiIndex.from_tuples([(0,0,0)], names=['subject_id', 'icustay_id', 'hour'])
    empty_df = pd.DataFrame(data={}, index=empty_idx).head(0)
    with pd.HDFStore('mimic.h5') as hdf:
        for i, t in enumerate(tables):
            print(t, '...')
            verify_record_length(hdf, t, total_len[i], empty_df)

def join_hdf():
    with pd.HDFStore('mimic.h5') as hdf:
        for c in [0] + CUTOFFS:
            print('Joining cutoff', c)
            hour_limits = collections.defaultdict(lambda: [100000, -100000], {})
            ts = {}
            for t in tables:
                tab = hdf.get(k_table_cutoff(t, c))
                tab.sort_index(inplace=True, kind='mergesort')
                hdf.put(k_table_cutoff(t, c), tab)
                for i in tab.index:
                    hour_limits[i[:2]][0] = min(hour_limits[i[:2]][0], i[2])
                    hour_limits[i[:2]][1] = max(hour_limits[i[:2]][1], i[2])
            del tab
            hit = HourIterator(hour_limits)
            joined_idx = pd.MultiIndex.from_tuples(iter(hit), names=['subject_id', 'icustay_id', 'hour'])
            joined = pd.DataFrame(data={}, index=joined_idx)
            del hit, joined_idx

            for t in tables:
                tab = hdf.get(k_table_cutoff(t, c))
                #joined = joined.merge(ts[t], how='outer', left_index=True, right_index=True)
                joined = joined.join(tab, how='left')
                del tab
            hdf.put(k_table_cutoff('joined', c), joined)
        zero_headers = []
        for t in tables:
            if t != 'chartevents':
                zero_headers += list(ts[t].keys())
        hdf.put('/zero_headers', pd.Series(zero_headers))

if __name__ == '__main__':
    create_hdf()
    join_hdf()
