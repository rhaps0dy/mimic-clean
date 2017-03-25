#!/usr/bin/env python3

import psycopg2
import sys
import pickle
import re
import itertools as it
from memoize_pickle import memoize_pickle

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

ex_float = re.compile("([+-]?[0-9]*\\.?[0-9]+)")

def list_categories(cursor, table, max_n_categories=20, window_size=100000):
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
    cursor.itersize = window_size
    cursor.execute(("SELECT t.itemid, value FROM {:s} t "
                    "ORDER BY t.itemid;")
                    .format(table))
    for window_i in it.count():
        at_least_one_item = False
        print("Iterating...")
        _i = 0
        for itemid, value in cursor:
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
                continue
            match = ex_float.search(value)
            if match is None:
                item_is_number = False
                if value not in categories:
                    categories[value] = len(categories)
            else:
                n = float(match.group(1))
                n_numeric_values += 1
                #if len(match.group(1)) < len(value):
                #    print("Non-full number \"{:s}\" for item {:d} {:s}"
                #          .format(value, itemid, label))

        if not at_least_one_item:
            close_item(itemid, label, item_is_number, item_all_are_None, categories, item_categories, n_numeric_values, n_total_values)
            break

    return item_categories

def get_lister_function(table):
    @memoize_pickle("{:s}_item_categories.pkl".format(table))
    def f(cursor, **kwargs):
        return list_categories(cursor, table, **kwargs)
    return f

if __name__ == "__main__":
    conn_string = "host='localhost' dbname='adria' user='adria' password='adria'"
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("SET search_path TO mimiciii;")
    n_cursor = conn.cursor('n_cursor')
    #get_lister_function('chartevents')(n_cursor)
    #get_lister_function('outputevents')(n_cursor)
    #get_lister_function('labevents')(n_cursor)
