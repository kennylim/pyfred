
INSERT INTO fred.observation (series, realtime_start, realtime_end, observation_date, value)
SELECT series,
       realtime_start ::TIMESTAMP ::date,
       realtime_end ::TIMESTAMP ::date,
       observation_date ::TIMESTAMP ::date,
       CASE
           WHEN value = '.' THEN NULL
           ELSE value ::varchar ::numeric
       END
FROM fred.observation_staging;
