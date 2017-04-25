# MIMIC-clean
Scripts to clean up the MIMIC data set to analyse it. Instructions:

## labevents.csv
  - Run `python extract_events.py labevents` to generate `labevents_item_categories.pkl`.
  - Run the following SQL:
      sql```
        COPY (SELECT itemid, label, category FROM mimiciii.d_labitems
        ) TO '/tmp/d_labitems.csv' DELIMITER ',' CSV HEADER;
      ```
    And then the command `cp /tmp/d_labitems.csv .`
  - Run `python labevents_generate_possible_values.py` to generate `GEN_labevents_clean.py`.
  - Run `python create_events_table.py labevents` to generate `labevents.csv`
