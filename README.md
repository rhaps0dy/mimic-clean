# MIMIC-clean
Scripts to clean up the MIMIC data set to analyse it. Not including
microbiology events or caregivers.

## Putting MIMIC in your database
MIMIC comes in the form of a few `csv.gz` files. To make this easier to work
with, it is helpful to put it in a PostgreSQL database. The steps to do this
are:

1. Install PostgreSQL. `sudo apt install postgresql postgresql-client`
2. Download their repository https://github.com/MIT-LCP/mimic-code and navigate
   to `mimic-code/buildmimic/postgres`.
3. run `make create-user mimic-gz datadir="/path/to/raw/data" DBUSER="$(whoami)" DBPASS=password"`
4. Run `psql 'dbname=mimic'
   mimic-code/concepts/durations/ventilation-durations.sql`. If you want to
   predict things that are not ventilation, it's probably helpful to run all the
   other concepts (by using the file `concepts/make-concepts.sql`)

Once this is done, the cleaning scripts can run.

## Generating the clean data CSV

To generate the clean data set, first, make sure you are in a Python3
environment or virtualenv. Then run:

```sh
./clean.bash
```

and the script will do everything for you. Intermediate files and
other tables will be in the `cache` folder.
