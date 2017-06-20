####
# This script sorts CSV files assuming they're already sorted by subject_id.
####

import csv
import pickle
import sys
import os
import collections
import math

def sort_csv(infname, outfname):
    with open(infname, 'r', newline='') as csvf_in:
        with open(outfname, 'w', newline='') as csvf_out:
            inp = iter(csv.reader(csvf_in))
            out = csv.writer(csvf_out)
            out.writerow(next(inp)) # headers

            icustay_id_waiting = collections.defaultdict(lambda: [], {})
            def dump_icustays():
                for idw in sorted(list(icustay_id_waiting.keys())):
                    prev_h = -math.inf
                    for r in icustay_id_waiting[idw]:
                        h = float(r[2])
                        assert h > prev_h
                        prev_h = h
                        out.writerow(r)
                    del icustay_id_waiting[idw]

            prev_subject_id = 0
            for row in inp:
                subject_id, icustay_id = map(int, row[:2])
                assert subject_id >= prev_subject_id, "subject_id is sorted"

                if subject_id != prev_subject_id:
                    dump_icustays()
                    prev_subject_id = subject_id
                icustay_id_waiting[icustay_id].append(row)

            dump_icustays()

def join_csvs(left_fn, right_fn, out_fn, priority=None):
    with open(left_fn, 'r', newline='') as csvf_left:
        with open(right_fn, 'r', newline='') as csvf_right:
            with open(out_fn, 'w', newline='') as csvf_out:
                left = iter(csv.reader(csvf_left))
                right = iter(csv.reader(csvf_right))
                out = csv.writer(csvf_out)

                left_header = next(left)
                right_header = next(right)

                right_removed = []
                if priority == 'left':
                    for i, h in enumerate(right_header[3:]):
                        if h in left_header:
                            right_removed.append((i+3, left_header.index(h)))
                if priority == 'right':
                    raise NotImplementedError("Requires code duplication, not needed")
                right_removed.sort(key=lambda t: -t[0])

                for r, _ in right_removed:
                    del right_header[r]

                out.writerow(left_header+right_header[3:])
                left_empty = [None]*(len(left_header)-3)
                right_empty = [None]*(len(right_header)-3)

                try:
                    lrow = next(left)
                    rrow = next(right)
                    while True:
                        tl = (int(lrow[0]), int(lrow[1]), float(lrow[2]))
                        tr = (int(rrow[0]), int(rrow[1]), float(rrow[2]))
                        if tl < tr:
                            out.writerow(lrow + right_empty)
                            lrow = next(left)
                        elif tl > tr:
                            le = left_empty
                            for r, l in right_removed:
                                if rrow[r] != '':
                                    if le is left_empty:
                                        le = left_empty.copy()
                                    le[l] = rrow[r]
                                del rrow[r]
                            out.writerow(rrow[:3] + le + rrow[3:])
                            rrow = next(right)
                        else:
                            for r, l in right_removed:
                                if rrow[r] != '' and lrow[l] == '':
                                    lrow[l] = rrow[r]
                                del rrow[r]
                            out.writerow(lrow + rrow[3:])
                            lrow = next(left)
                            rrow = next(right)
                except StopIteration:
                    for row in left:
                        out.writerow(row + right_empty)
                    for row in right:
                        out.writerow(row[:3] + left_empty + row[3:])

def sort_files(fnames):
    children = []
    for f in fnames:
        newpid = os.fork()
        children.append(newpid)
        if newpid == 0:
            sort_csv(f, f+'.1')
            sys.exit(0)
    for child in children:
        os.waitpid(child, 0)

def join_files(fnames, priority=None):
    width = 1
    while width < len(fnames):
        width *= 2
        children = []
        for i in range(0, len(fnames)-width//2, width):
            newpid = os.fork()
            children.append(newpid)
            if newpid == 0:
                fname_left = "{:s}.{:d}".format(fnames[i], width//2)
                fname_right = "{:s}.{:d}".format(fnames[i+width//2], width//2)
                fname_out = "{:s}.{:d}".format(fnames[i], width)
                if priority == fnames[i]:
                    pr = 'left'
                elif priority == fnames[i+width//2]:
                    pr = 'right'
                else:
                    pr = None
                join_csvs(fname_left, fname_right, fname_out, pr)
                sys.exit(0)
        i += width
        if i < len(fnames):
            os.rename("{:s}.{:d}".format(fnames[i], width//2), "{:s}.{:d}".format(fnames[i], width))
        for child in children:
            os.waitpid(child, 0)

if __name__ == '__main__':
    d = {}
    i = 1
    if '--priority' in sys.argv:
        priority = sys.argv.index('--priority')
        d['priority'] = sys.argv[priority+1]
        i = max(i, priority+2)
    sort_files(sys.argv[i:])
    join_files(sys.argv[i:], **d)
