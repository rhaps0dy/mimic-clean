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
import os

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

ex_float = re.compile(
    r"^.*([+-]?[0-9]+\.[0-9]+).*| *([+-]?(\.[0-9]+|[0-9]+\.?)) *$")

@pu.memoize("{0:s}_item_categories.pkl.gz")
def list_categories(table, cursor, _cursor, max_n_categories=20, window_size=100000):
    def close_item(itemid, item_is_number, item_all_are_None, categories, item_categories, n_numeric_values, n_total_values):
        item_categories[itemid] = {'categories': categories,
                'frequency': n_total_values}
        if item_all_are_None:
            print("All instances of {:d} ({:d}) are None".format(itemid, n_total_values))
        elif item_is_number or n_numeric_values > len(categories):
            print("Item {:d} is a real (total {:d} instances, {:d} distinct values, plus {:d} categories)"
                    .format(itemid, n_total_values, n_numeric_values, len(categories)))
            categories[float] = n_numeric_values
        elif len(categories) <= max_n_categories:
            print("Item {:d} ({:d} times) is categorical: {:s}"
                    .format(itemid, n_total_values, str(categories)))
        else:
            print("Error with item {:d}".format(itemid))

    translation = pu.load("translation_{:s}.pkl.gz".format(table))
    item_categories = {}
    prev_itemid = None
    print("Executing query...")
    cursor.itersize = window_size
    if table == 'labevents':
        cursor.execute(("SELECT itemid, value, valuenum FROM labevents "
                        "WHERE subject_id IN ( "
                        "    SELECT subject_id FROM selected_patients) "
                        "ORDER BY itemid"))
    elif table == 'chartevents':
        cursor.execute(("SELECT itemid, value, valuenum FROM chartevents "
                        "WHERE hadm_id IN ( "
                        "    SELECT hadm_id FROM selected_patients) "
                        "ORDER BY itemid"))
    prev_encounters = {}
    for window_i in it.count():
        at_least_one_item = False
        print("Iterating...")
        for itemid, value, valuenum in cursor:
            if itemid in translation:
                itemid = translation[itemid]
            at_least_one_item = True
            if itemid != prev_itemid:
                if prev_itemid is not None:
                    close_item(prev_itemid, item_is_number,
                               item_all_are_None, categories, item_categories,
                               n_numeric_values, n_total_values)
                    prev_encounters[prev_itemid] = (item_is_number,
                                                    item_all_are_None,
                                                    categories,
                                                    printed_message_spurious,
                                                    n_numeric_values,
                                                    n_total_values)
                prev_itemid = itemid
                if itemid in prev_encounters:
                    (item_is_number, item_all_are_none, categories,
                        printed_message_spurious, n_numeric_values,
                        n_total_values) = prev_encounters[itemid]
                else:
                    item_is_number = True
                    item_all_are_None = True
                    categories = {}
                    printed_message_spurious = False
                    n_numeric_values = 0
                    n_total_values = 0
            n_total_values += 1
            if value == 'Unable to Assess':
                print(valuenum)
            if value is not None:
                item_all_are_None = False
            else:
                if not item_all_are_None and not printed_message_spurious:
                    print("Item {:d} has some spurious Nones".format(itemid))
                    printed_message_spurious = True
                if valuenum is not None:
                    n_numeric_values += 1
                continue
            match = ex_float.fullmatch(value)
            if valuenum is not None:
                n_numeric_values += 1
                if match is not None:
                    continue
                elif itemid != 228303:
                    print("Valuenum is {:f} and value is {:s}, item {:d}"
                          .format(valuenum, value, itemid))
            if match is None:
                item_is_number = False
                if value not in categories:
                    categories[value] = len(categories)
            else:
                gs = match.groups()
                n = float(gs[0] or gs[1])
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
                #          .format(value, itemid))

        if not at_least_one_item:
            close_item(itemid, item_is_number, item_all_are_None, categories, item_categories, n_numeric_values, n_total_values)
            break

    return item_categories

if __name__ == "__main__":
    assert sys.argv[1] in {'labevents', 'chartevents'}

    conn = psycopg2.connect(os.environ["CONN_STRING"])
    cursor = conn.cursor()
    cursor.execute("SET search_path TO mimiciii;")
    n_cursor = conn.cursor('n_cursor_'+sys.argv[1])
    list_categories(sys.argv[1], n_cursor, cursor)
