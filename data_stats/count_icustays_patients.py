import gzip
import csv
import pickle_utils as pu
from tqdm import tqdm

MIMIC_PATH = '../../mimic.csv.gz'

@pu.memoize('dataset/subject_id_icustays.pkl.gz')
def subject_id_icustays():
    _, n_lines = pu.load('dataset/number_non_missing.pkl.gz')
    s = set()
    with gzip.open(MIMIC_PATH, 'rt') as gzf:
        f = csv.reader(gzf)
        headers = next(f)
        for _ in tqdm(range(n_lines)):
            line = next(f)
            s.add(tuple(line[:2]))
    return s

if __name__ == '__main__':
    s = subject_id_icustays()
    sis, ics = zip(*s)
    print("Number of subject_ids:", len(set(sis)))
    print("Number of icustay_ids:", len(set(ics)))
    print("Distinct pairs:", len(s))

