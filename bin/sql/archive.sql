--------------------------------------
--  test new record

UPDATE observation_staging
SET flag = NULL;


INSERT INTO observation_staging (series, realtime_start, realtime_end, observation_date, value)
VALUES ('UNRATE',
        '2016-04-29',
        '2016-04-29',
        '2016-05-09',
        '.');


INSERT INTO observation_staging (series, realtime_start, realtime_end, observation_date, value)
VALUES ('GDPC1',
        '2016-04-29',
        '2016-04-29',
        '2016-05-09',
        '1.7');


INSERT INTO observation_staging (series, realtime_start, realtime_end, observation_date, value)
VALUES ('GDPC1',
        '2016-04-29',
        '2016-04-29',
        '2016-05-09',
        '7.7');


SELECT *
FROM observation
WHERE row_indicator = 'current';


SELECT *
FROM observation_staging
WHERE flag = 1;


SELECT *
FROM observation_staging
WHERE flag IS NULL;


SELECT *
FROM observation
WHERE series = 'GDPC1'
    AND observation_date = '2016-05-09'
    DELETE
    FROM observation_staging WHERE series = 'GDPC1';


SELECT *
FROM observation
WHERE series = 'GDPC1'
    AND observation_date = '1947-01-01'
