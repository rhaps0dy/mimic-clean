#!/bin/bash

pip install -r requirements.txt

# Create `selected_patients` in the database
psql < static_file.sql
cp /tmp/selected_patients.csv .

# Create drug views in the database
# (optional unless you want to use long drug intervals)
python create_drug_durations.py
for f in DRUGS/*; do
	psql < $f
done

# Go to directory cache
mkdir cache
pushd cache

# Create files that map duplicate columns within each table with each other
python ../create_translations.py

# Enumerate all possible values of chartevents and labevents, to determine
# whether a column is float or categorical, and to clean misspelled data
python ../item_categories.py chartevents &
python ../item_categories.py labevents &
wait

# The generated value for labevents has to be put in another file
python ../labevents_generate_possible_values.py
mv GEN_labevents_clean.py ..

# Create tables
python ../create_events_table.py chartevents &
python ../create_events_table.py outputevents &
python ../create_events_table.py labevents &
python ../create_events_table.py drugevents &
python ../create_events_table.py procedureevents_mv &
wait

# Join all the tables into one
python ../join_csv.py --priority labevents.csv labevents.csv chartevents.csv
mv labevents.csv.2 lab_chart.csv
python ../join_csv.py drugevents.csv procedureevents_mv.csv outputevents.csv lab_chart.csv

mv drugevents.csv.4 ../mimic.csv

popd
