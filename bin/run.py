#!/usr/bin/python

import os
import time

series = ['UNRATE', 'UMCSENT', 'GDPC1']
now  = time.strftime("%Y%m%d_%H%M%S")

for series_id in series:
    # fetch json data from fred rest api and generate cvs datafile
    os.system('python populate_csv.py -s %s' % series_id)
    print '%s series datafile created' % series_id

    # truncate staging table
    os.system('psql -d postgres -f sql/truncate_observation_staging.sql')
    print 'Staging table truncated'

    # load datafile into staging table
    os.system("""
              psql -d postgres -c "COPY observation_staging
              (series,realtime_start, realtime_end, observation_date, value)
              FROM '/Users/postgres/fred/data/fred_series_%s.csv'
              WITH (ENCODING 'utf-8', HEADER 1,FORMAT 'csv');"
              """ % series_id)
    print '%s series data loaded into staging table' % series_id

    # validate and flag staging table
    os.system('psql -d postgres -f sql/validate_observation_staging.sql')
    print '%s series validated and tagged on staging table' % series_id

    # transform data from staging and load into observation table
    os.system('psql -d postgres -f sql/transform_and_load_observation_table_delta.sql')
    print '%s series data transformed and loaded into observation table' % series_id

    # transform data from staging and load into observation table
    os.system('psql -d postgres -f sql/load_report.sql')
    print '%s series report generated' % series_id

    # rename and move datafile with timestamp from data to archive director
    os.system('gzip -c ../data/fred_series_%s.csv > ../data/fred_series_%s.csv.gz' % (series_id, series_id))
    os.system('mv ../data/fred_series_%s.csv.gz ../archive/fred_series_%s_%s.csv.gz' % (series_id, series_id, now))
    os.system('rm ../data/fred_series_%s.csv' % series_id)
    print 'Move fred_series_%s_%s to archive directory ' % (series_id, now)
