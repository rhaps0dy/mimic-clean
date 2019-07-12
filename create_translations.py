import psycopg2
import pickle_utils as pu
import itertools as it
import collections
import os

def translation(iterable):
    d = {}
    t = {}
    for label, itemid in iterable:
        if label not in d:
            d[label] = itemid
        t[itemid] = d[label]
    return t


@pu.memoize("translation_{1:s}.pkl.gz")
def translation_d_items(cursor, table):
    command = ("SELECT label, itemid FROM d_items WHERE dbsource='metavision' "
               "AND linksto='{:s}' AND label IN (SELECT label FROM d_items "
               "    WHERE dbsource='metavision' AND linksto='{:s}' "
               "    GROUP BY label HAVING COUNT(*)>1);").format(table, table)
    cursor.execute(command)
    return translation(cursor)

@pu.memoize("translation_labevents.pkl.gz")
def translation_d_labitems(cursor):
    command = ("SELECT label, fluid, itemid FROM d_labitems WHERE label IN ("
               "    SELECT label FROM d_labitems "
               "    GROUP BY label HAVING COUNT(*)>1);")
    cursor.execute(command)
    f = lambda t: ((t[0], t[1]), t[2])
    return translation(map(f, cursor))

@pu.memoize("item_names.pkl.gz")
def item_names(cursor):
    cursor.execute("SELECT itemid, label FROM d_items WHERE itemid > 220000")
    a = cursor.fetchall()
    cursor.execute("SELECT itemid, label, fluid FROM d_labitems")
    b = map(lambda t: (t[0], t[1]+' - '+t[2]), cursor.fetchall())
    return dict(it.chain(a, b))

if __name__ == '__main__':
    conn = psycopg2.connect(os.environ["CONN_STRING"])
    cursor = conn.cursor()
    cursor.execute("SET search_path TO mimiciii")

    # Some items are duplicated between (inputevents_mv or inputevents_cv) and
    # labevents. We ignore that and put both of them in the result)

    # Some items are duplicated within the same table (most in chartevents, one in
    # inputevents_mv, a few in labevents). Since we output tables row-major, we can
    # deal with those at table construction time

    # Some items are duplicated between chartevents and labevents. For those, when
    # one of them is missing, the other one is used, otherwise labevents is used.
    # We can apply that as a postprocessing step to the CSVs. We could also do it
    # at construction time but that would restrict us to 1 core

    # The duplication in `labevents` is computed taking into account both the
    # `label` and `fluid` fields. However to compute the cross-duplication with
    # `chartevents` we need only check the `label` field (this last thing has been
    # checked manually)

    tc = translation_d_items(cursor, 'chartevents')
    translation_d_items(cursor, 'inputevents_mv')
    tl = translation_d_labitems(cursor)
    item_names(cursor)
