#!/usr/bin/python

import csv
import json
import sys
import argparse
import datetime
import os
from urllib2 import Request, urlopen, URLError

# accept series id argument
parser = argparse.ArgumentParser()
parser.add_argument('-s', '--series', action='store', dest='series_id',
                    required=True, help="enter series id to retrieve \
                    data from fred rest api and populate datafile")
args = parser.parse_args()
series_id = args.series_id


# parameters
rest_api_url = 'https://api.stlouisfed.org/fred/series/observations'
rest_api_key = 'e7101c099c55403cefe869056e99a9fd'
file_type = 'json'
now = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")  # ISO 8601 data format
csv_datafile = 'fred_series_' + series_id + '.csv'
data_dir = '../data'
request = Request('%s?series_id=%s&api_key=%s&file_type=%s' %
                  (rest_api_url, series_id, rest_api_key, file_type))


def fetch_rest_data():
    try:
        response = urlopen(request)
        rest_data = response.read()
        return rest_data

    except URLError, e:
        print 'No %s REST Data Found. Error Message %s' % (series_id, e)
        sys.exit(1)


def parse_json_data(data):
    try:
        json_parsed = json.loads(data)
        json_data = json_parsed['observations']  # json key
        return json_data

    except ValueError:
        print 'Unable to Parse JSON Data.'
        sys.exit(1)


def create_csv_datafile(json_data):
    try:
        # create csv datafile
        csv_out = open(os.path.join(data_dir, csv_datafile), 'wb')
        writer = csv.writer(csv_out)
        fields = ['series_id', 'realtime_start',
                  'realtime_end', 'date', 'value']
        writer.writerow(fields)  # writes field

        # convert json to csv
        for line in json_data:
            # writes a row and gets the fields from the json object
            writer.writerow([series_id,  # add series_id
                            line.get('realtime_start'),
                            line.get('realtime_end'),
                            line.get('date'),
                            line.get('value')])
        csv_out.close()

    except ValueError:
        print 'Unable to Generate CSV Data File.'
        sys.exit(1)


def main():

        data = fetch_rest_data()  # fetch data from rest api
        json_data = parse_json_data(data)  # parse json data
        create_csv_datafile(json_data)  # convert json to csv data file

if __name__ == "__main__":
    sys.exit(main())
