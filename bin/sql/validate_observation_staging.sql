 /*
1 = insert (except) : if not exists
2 = update old and insert new (minus) : if exist but different
3 = ignore (intersect) : if exist and the same
4 = bad record : if new record but invalid (i.e null on required column)
*/ -- records are the same - tag as ignore
 -- flag to insert new record

UPDATE observation_staging
SET flag = 1,
    descriptions = 'insert new record'
FROM
    (SELECT series,
            observation_date ::TIMESTAMP ::date
     FROM fred.observation_staging
     EXCEPT SELECT series,
                   observation_date
     FROM fred.observation
     ORDER BY series,
              observation_date) AS o
WHERE observation_staging.series = o.series
    AND observation_staging.observation_date ::TIMESTAMP ::date = o.observation_date;

 -- record exist but not the same between staging and observation
 --

UPDATE observation_staging
SET flag = 2,
    descriptions = 'update existing record'
FROM
    (SELECT s.series,
            s.observation_date ::TIMESTAMP ::date,
            CASE
                WHEN s.value = '.' THEN NULL
                ELSE s.value ::varchar ::numeric
            END
     FROM observation_staging s
     JOIN observation o ON s.series = o.series
     AND s.observation_date ::TIMESTAMP ::date = o.observation_date
     WHERE s.series = o.series
         AND s.observation_date ::TIMESTAMP ::date = o.observation_date
         AND s.value ::varchar ::numeric != o.value) AS o
WHERE observation_staging.series = o.series
    AND observation_staging.observation_date ::TIMESTAMP ::date = o.observation_date;

 -- record exist and the same on both staging and observation

UPDATE observation_staging
SET flag = 3,
    descriptions = 'ignore same record'
FROM
    (SELECT series,
            observation_date ::TIMESTAMP ::date,
            CASE
                WHEN value = '.' THEN NULL
                ELSE value ::varchar ::numeric
            END
     FROM fred.observation_staging INTERSECT SELECT series,
                                                    observation_date,
                                                    value
     FROM fred.observation
     ORDER BY series,
              observation_date) AS o
WHERE observation_staging.series = o.series
    AND observation_staging.observation_date ::TIMESTAMP ::date = o.observation_date;

 -- flag error

UPDATE observation_staging
SET flag = 4,
    descriptions = 'missing value in required column'
FROM
    (SELECT series,
            observation_date,
            CASE
                WHEN value = '.' THEN NULL
                ELSE value ::varchar ::numeric
            END
     FROM fred.observation_staging
     WHERE length(series) = 0
         OR length(observation_date) = 0
         OR length(value) = 0
         OR value = '.') s
WHERE observation_staging.series = s.series
    AND observation_staging.observation_date = s.observation_date;
