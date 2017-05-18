COPY "observation_staging" ("series", "realtime_start", "realtime_end", "observation_date","value")
    FROM '/project/fundingcircle/data/fred_series_UNRATE.csv'
    WITH (
        ENCODING 'utf-8',
        HEADER 1,
        FORMAT 'csv'
    );

