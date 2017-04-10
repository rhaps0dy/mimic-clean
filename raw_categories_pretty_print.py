#!/usr/bin/env python3

####
# This file pretty-prints the programmatically inferred raw categories for item
# ids, for manual cleaning
####

from do_one_ventilation import get_item_names, get_item_categories
import csv
import psycopg2
import pickle

if __name__ == '__main__':
    conn_string = "host='localhost' dbname='adria' user='adria' password='adria'"
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("SET search_path TO mimiciii;")

    item_names = get_item_names(cursor)
    item_categories = get_item_categories(cursor)

    with open('raw_categories.csv', 'w') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['itemid', 'name', 'categories'])
        for itemid, cats in item_categories:
            writer.writerow([itemid, item_names[itemid], str(cats)])


