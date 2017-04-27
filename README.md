# MIMIC-clean
Scripts to clean up the MIMIC data set to analyse it. Not including
microbiology events or caregivers. The instructions to generate the whole
dataset would be roughly as follows:

- Generate `chartevents_item_categories.pkl` using `extract_events.py chartevents`
- Generate one or two CSVs using `create_event_table.py`
- Create the `icustay_indices.pkl` file using `inflate_icustay_indices.py`
- Re-generate the CSVs, generate all possible CSVs from `create_event_table.py`
- Join the CSVs by using `join_csvs.py`

## labevents.csv
  - Run `python extract_events.py labevents` to generate `labevents_item_categories.pkl`.
  - Run the following SQL:
      ```sql
        COPY (SELECT itemid, label, category FROM mimiciii.d_labitems
        ) TO '/tmp/d_labitems.csv' DELIMITER ',' CSV HEADER;
      ```
    And then the command `cp /tmp/d_labitems.csv .`
  - Run `python labevents_generate_possible_values.py` to generate `GEN_labevents_clean.py`.
  - Run `python create_events_table.py labevents` to generate `labevents.csv`
