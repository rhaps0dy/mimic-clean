#!/usr/bin/env python3

####
# This file lists all the items and their possible values: whether they are
# categorical or numbers or something else
####

import psycopg2
import sys
import pickle
import re
import itertools as it
import pickle_utils as pu

# IGNORED: services, microbiologyevents, noteevents, prescriptions (relevant drugs are in inputevents)
#
# chartevents: all items have the same UoM, except "113 CVP", which is in % and in mmHg
# procedureevents_mv: items with different UoM have only UoMs 'day', 'hour', 'min'
# datetimeevents: no problemo (value is a date)
# inputevents_cv: madness, UoMs have to be converted (value already number, also rate to be considered)
# inputevents_mv: also madness (value already number, also rate to be considered)
# outputevents: no problemo (value is already number)
# labevents: Contains "GREATER THAN", "LESS THAN" in numbers, also <0.0 and >0.0
#    - 50889: mg/L and mg/dL
#    - 50916: ug/dl and ng/ml
#    - 50958: mIU/mL and mIU/L
#    - 50964: mOsm/kg and MOSM/L. Density of blood plasma 1.025g/ml (https://en.wikipedia.org/wiki/Osmotic_concentration#Plasma_osmolarity_vs._osmolality)
#    - 50980: IU/mL and I.U.
#    - 50989: pg/mL and ng/DL
#    - 51127: #/uL and #/CU MM which are equivalent
#    - 51128: #/uL and #/CU MM which are equivalent
#    - 51464: EU/dl and mg/dl which are equivalent -- What is SM and MOD?
#    - 51514: EU/dL and mg/dL, which are equivalent

ex_float = re.compile("([+-]?[0-9]+\\.[0-9]+)")
METAVISION_MIN_ID = 220000
MIN_AGE = 14

def get_newborns(cursor, table, patient_id):
    cursor.execute("SELECT {:s} FROM static_icustays WHERE r_age < %s"
                   .format(patient_id), [MIN_AGE])
    return set(e for e, in cursor.fetchall())

@pu.memoize("{0:s}_item_categories.pkl.gz")
def list_categories(table, cursor, _cursor, max_n_categories=20, window_size=100000):
    def close_item(itemid, label, item_is_number, item_all_are_None, categories, item_categories, n_numeric_values, n_total_values):
        item_categories[itemid] = {'categories': categories,
                'frequency': n_total_values}
        if item_all_are_None:
            print("All instances of {:d} {:s} ({:d}) are None".format(itemid, label, n_total_values))
        elif item_is_number or n_numeric_values > len(categories):
            print("Item {:d} {:s} is a real (total {:d} instances, {:d} distinct values, plus {:d} categories)"
                    .format(itemid, label, n_total_values, n_numeric_values, len(categories)))
            categories[float] = n_numeric_values
        elif len(categories) <= max_n_categories:
            print("Item {:d} {:s} ({:d} times) is categorical: {:s}"
                    .format(itemid, label, n_total_values, str(categories)))
        else:
            print("Error with item {:d}".format(itemid))

    item_categories = {}
    prev_itemid = None
    print("Executing query...")
    if table == 'labevents':
        patient_id = 'subject_id'
    else:
        patient_id = 'icustay_id'
    newborn_icustay_ids = get_newborns(_cursor, table, patient_id)
    cursor.itersize = window_size
    cursor.execute(("SELECT {:s}, t.itemid, value, valuenum FROM {:s} t "
                    "ORDER BY t.itemid;")
                    .format(patient_id, table))
    for window_i in it.count():
        at_least_one_item = False
        print("Iterating...")
        _i = 0
        for icustay_id, itemid, value, valuenum in cursor:
            if icustay_id in newborn_icustay_ids:
                continue

            _i += 1
            if _i > window_size:
                break
            label = ''
            at_least_one_item = True
            if itemid != prev_itemid:
                if prev_itemid is not None:
                    close_item(prev_itemid, prev_label, item_is_number,
                               item_all_are_None, categories, item_categories, n_numeric_values, n_total_values)
                prev_itemid = itemid
                prev_label = label
                item_is_number = True
                item_all_are_None = True
                categories = {}
                printed_message_spurious = False
                n_numeric_values = 0
                n_total_values = 0
            n_total_values += 1
            if value is not None:
                item_all_are_None = False
            else:
                if not item_all_are_None and not printed_message_spurious:
                    print("Item {:d} {:s} has some spurious Nones".format(itemid, label))
                    printed_message_spurious = True
                if valuenum is not None:
                    n_numeric_values += 1
                continue
            if valuenum is not None:
                n_numeric_values += 1
                continue
            match = ex_float.search(value)
            if match is None:
                item_is_number = False
                if value not in categories:
                    categories[value] = len(categories)
                if valuenum is not None:
                    print("WTF, valuenum is {:f} and value is {:s}, item {:d} {:s}".format(valuenum, value, itemid, label))
            else:
                n = float(match.group(1))
                n_numeric_values += 1
                try:
                    float(value)
                except ValueError:
                    if value not in categories:
                        categories[value] = len(categories)
                else:
                    n_numeric_values += 1
                #if len(match.group(1)) < len(value):
                #    print("Non-full number \"{:s}\" for item {:d} {:s}"
                #          .format(value, itemid, label))

        if not at_least_one_item:
            close_item(itemid, label, item_is_number, item_all_are_None, categories, item_categories, n_numeric_values, n_total_values)
            break

    return item_categories

if __name__ == "__main__":
    conn_string = "host='localhost' dbname='adria' user='adria' password='adria'"
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("SET search_path TO mimiciii;")
    n_cursor = conn.cursor('n_cursor')
    #list_categories('chartevents', n_cursor, cursor)
    #list_categories('outputevents', n_cursor, cursor)
    list_categories('labevents', n_cursor, cursor)
