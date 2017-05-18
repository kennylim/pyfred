
Directory Structure

* bin  (source code )
* data (datafile downloaded from fred rest api)
* archive (compressed old datafile)
* report (reports)
* doc (documentations)

Manual Installation

1. Save and uncompress provided fred.zip file in your local postgres home directory

su - postgres
cd ~/fred/bin

2. Connect to database via psql to create fred schema, staging table observation table
psql -d postgres -f sql/create_observation_schema.sql

3. Execute python script to fetch series data from fred api and convert to csv datafile

python populate_csv.py -s UNRATE
python populate_csv.py -s UMCSENT
python populate_csv.py -s GDPC1

4. Truncate Staging  (skip this step if staging is empty. there are no prior load)
psql -d postgres -f sql/truncate_observation_staging.sql

5. Load cvs data into staging table

Note: You will have to change the datafile directory path
if it different from your environemnt.

psql -d postgres -c "COPY observation_staging
              (series,realtime_start, realtime_end, observation_date, value)
              FROM '/Users/postgres/fred/data/fred_series_UNRATE.csv'
              WITH (ENCODING 'utf-8', HEADER 1,FORMAT 'csv')";

psql -d postgres -c "COPY observation_staging
              (series,realtime_start, realtime_end, observation_date, value)
              FROM '/Users/postgres/fred/data/fred_series_UMCSENT.csv'
              WITH (ENCODING 'utf-8', HEADER 1,FORMAT 'csv')";

psql -d postgres -c "COPY observation_staging
              (series,realtime_start, realtime_end, observation_date, value)
              FROM '/Users/postgres/fred/data/fred_series_GDPC1.csv'
              WITH (ENCODING 'utf-8', HEADER 1,FORMAT 'csv')";

6. Validate and flag staging table
psql -d postgres -f sql/validate_observation_staging.sql

7. Transform data in staging and load into observation table

psql -d postgres -f sql/transform_and_load_observation_table.sql

8. Generate average annual unemployment rate report to report directory

psql -d postgres -f sql/annual_unemployment_average.sql  -o ../report/annual_unemployment_average_report.txt


Quick Installation

1. Save and extract fred tar file in user home directory

su - postgres
tar -xvf fred.tar
cd ~/fred/bin

2. Connect to database via psql to create fred schema, staging table observation table
psql -d postgres -f sql/create_observation_schema.sql

3. I wrote a simple automation script to do the following tasks:

* Fetch json data from Fred Rest API
* Convert into cvs datafile
* Truncate staging table
* Load cvs data into staging table
* Transform and load data from staging to observation table

Note: You will have to verify and have to change the load datafile path to your environment if is diferent. The path is
in line 22 from run.py script

   FROM '/Users/postgres/fred/data/fred_series_%s.csv'

python run.py

4. Generate average annual unemployment rate report to report directory

psql -d postgres -f sql/annual_unemployment_average.sql  -o ../report/annual_unemployment_average_report.txt

5. To schedule to run midnight everyday
crontab -e

0 00 * * * /Users/postgres/fred/bin/run.py
