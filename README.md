# MIMIC-clean
Scripts to clean up the MIMIC data set to analyse it. Not including
microbiology events or caregivers. To generate the whole data set,
first, make sure you are in a Python3 environment or virtualenv. Then
run:

```sh
./clean.bash
```

and the script will do everything for you. Intermediate files and
other tables will be in the `cache` folder.
