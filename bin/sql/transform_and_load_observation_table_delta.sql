 -- insert new record

INSERT INTO fred.observation (series, realtime_start, realtime_end, observation_date, value, row_indicator)
SELECT series,
       realtime_start ::TIMESTAMP ::date,
       to_timestamp('9999-12-31', 'yyyy-MM-dd') ::TIMESTAMP ::date,
       observation_date ::TIMESTAMP ::date,
       CASE
           WHEN value = '.' THEN NULL
           ELSE value ::varchar ::numeric
       END,
       'current'
FROM fred.observation_staging
WHERE flag = 1;

 -- expire old record (type 2 scd)

UPDATE fred.observation
SET realtime_end = date_trunc('day', now()),
                   row_indicator = 'expired'
FROM
    (SELECT s.series,
            s.observation_date
     FROM observation_staging s
     WHERE flag = 2) AS s
WHERE observation.series = s.series
    AND observation.observation_date = s.observation_date ::TIMESTAMP ::date
    AND observation.row_indicator = 'current';

 -- insert new record (type 2 scd)

INSERT INTO fred.observation (series, realtime_start, realtime_end, observation_date, value, row_indicator)
SELECT series,
       realtime_start ::TIMESTAMP ::date,
       to_timestamp('9999-12-31', 'yyyy-MM-dd') ::TIMESTAMP ::date,
       observation_date ::TIMESTAMP ::date,
       CASE
           WHEN value = '.' THEN NULL
           ELSE value ::varchar ::numeric
       END,
       'current'
FROM fred.observation_staging
WHERE flag = 2;
