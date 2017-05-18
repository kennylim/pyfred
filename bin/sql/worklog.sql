
-- worklog
\d
\d observation
\d observation_staging

 TRUNCATE fred.observation_staging;

 TRUNCATE fred.observation;



 ------------------------------

 crontab -e
 0 00 * * * /Users/postgres/fred/bin/run.py

--INSERT NEW, skip EXISTING AND flag error (flag: 1,3,4)
python run.py
SELECT *
FROM fred.observation_staging;

 python run.py
SELECT *
FROM fred.observation_staging;


-- flag 2
 TYPE 2 SCD: (flag 2)

 TRUNCATE fred.observation_staging;

 — NEW record
INSERT INTO observation_staging (series, realtime_start, realtime_end, observation_date, value)
VALUES ('GDPC1',
        '2016-05-10',
        '2016-05-10',
        '2016-05-10',
        '1.7');

select * from observation_staging ;

SELECT series,
       observation_date ::TIMESTAMP ::date
FROM fred.observation_staging
EXCEPT
SELECT series,
       observation_date
FROM fred.observation
ORDER BY series,
         observation_date;


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

select * from observation_staging;

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

select * from observation
where series = 'GDPC1'
and observation_date = '2016-05-10';

 ———————

TRUNCATE fred.observation_staging;

-- revision
INSERT INTO observation_staging (series, realtime_start, realtime_end, observation_date, value)
VALUES ('GDPC1',
        '2016-05-11',
        '2016-05-11',
        '2016-05-10',
        '7.7');

SELECT s.series,
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
    AND s.value ::varchar ::numeric != o.value;


-- staging table

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


select * from observation_staging;

--- observation table


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


SELECT *
FROM fred.observation
WHERE series = 'GDPC1'
    AND observation_date = '2016-05-10';


SELECT *
FROM fred.observation
    WHERE series = 'GDPC1'
    AND observation_date = '2016-05-10'
    AND row_indicator = 'current';

----

MacAir1:bin postgres$ python run.py
UNRATE series datafile created
TRUNCATE TABLE
Staging table truncated
COPY 820
UNRATE series data loaded into staging table
UPDATE 820
UPDATE 0
UPDATE 0
UPDATE 0
UNRATE series validated and tagged on staging table
INSERT 0 820
UPDATE 0
INSERT 0 0
UNRATE series data transformed and loaded into observation table
 flag |   descriptions    | count
------+-------------------+-------
    1 | insert new record |   820
(1 row)

UNRATE series report generated
Move fred_series_UNRATE_20160511_063148 to archive directory
UMCSENT series datafile created
TRUNCATE TABLE
Staging table truncated
COPY 551
UMCSENT series data loaded into staging table
UPDATE 551
UPDATE 0
UPDATE 0
UPDATE 92
UMCSENT series validated and tagged on staging table
INSERT 0 459
UPDATE 0
INSERT 0 0
UMCSENT series data transformed and loaded into observation table
 flag |           descriptions           | count
------+----------------------------------+-------
    1 | insert new record                |   459
    4 | missing value in required column |    92
(2 rows)

UMCSENT series report generated
Move fred_series_UMCSENT_20160511_063148 to archive directory
GDPC1 series datafile created
TRUNCATE TABLE
Staging table truncated
COPY 277
GDPC1 series data loaded into staging table
UPDATE 277
UPDATE 0
UPDATE 0
UPDATE 0
GDPC1 series validated and tagged on staging table
INSERT 0 277
UPDATE 0
INSERT 0 0
GDPC1 series data transformed and loaded into observation table
 flag |   descriptions    | count
------+-------------------+-------
    1 | insert new record |   277
(1 row)

-------------

MacAir1:bin postgres$ python run.py
UNRATE series datafile created
TRUNCATE TABLE
Staging table truncated
COPY 820
UNRATE series data loaded into staging table
UPDATE 0
UPDATE 0
UPDATE 820
UPDATE 0
UNRATE series validated and tagged on staging table
INSERT 0 0
UPDATE 0
INSERT 0 0
UNRATE series data transformed and loaded into observation table
 flag |    descriptions    | count
------+--------------------+-------
    3 | ignore same record |   820
(1 row)

UNRATE series report generated
Move fred_series_UNRATE_20160511_063223 to archive directory
UMCSENT series datafile created
TRUNCATE TABLE
Staging table truncated
COPY 551
UMCSENT series data loaded into staging table
UPDATE 92
UPDATE 0
UPDATE 459
UPDATE 92
UMCSENT series validated and tagged on staging table
INSERT 0 0
UPDATE 0
INSERT 0 0
UMCSENT series data transformed and loaded into observation table
 flag |           descriptions           | count
------+----------------------------------+-------
    3 | ignore same record               |   459
    4 | missing value in required column |    92
(2 rows)

UMCSENT series report generated
Move fred_series_UMCSENT_20160511_063223 to archive directory
GDPC1 series datafile created
TRUNCATE TABLE
Staging table truncated
COPY 277
GDPC1 series data loaded into staging table
UPDATE 0
UPDATE 0
UPDATE 277
UPDATE 0
GDPC1 series validated and tagged on staging table
INSERT 0 0
UPDATE 0
INSERT 0 0
GDPC1 series data transformed and loaded into observation table
 flag |    descriptions    | count
------+--------------------+-------
    3 | ignore same record |   277
(1 row)

GDPC1 series report generated
Move fred_series_GDPC1_20160511_063223 to archive directory
-------------


postgres=# select * from observation_staging;
 series | realtime_start | realtime_end | observation_date | value | flag |      descriptions
--------+----------------+--------------+------------------+-------+------+------------------------
 GDPC1  | 2016-05-11     | 2016-05-11   | 2016-05-11       | 7.7   |    2 | update existing record
(1 row)

postgres=# INSERT INTO fred.observation (series, realtime_start, realtime_end, observation_date, value, row_indicator)
postgres-# SELECT series,
postgres-#        realtime_start ::TIMESTAMP ::date,
postgres-#        to_timestamp('9999-12-31', 'yyyy-MM-dd') ::TIMESTAMP ::date,
postgres-#        observation_date ::TIMESTAMP ::date,
postgres-#        CASE
postgres-#            WHEN value = '.' THEN NULL
postgres-#            ELSE value ::varchar ::numeric
postgres-#        END,
postgres-#        'current'
postgres-# FROM fred.observation_staging
postgres-# WHERE flag = 2;
INSERT 0 1
postgres=#
postgres=# SELECT *
postgres-# FROM fred.observation
postgres-# WHERE series = 'GDPC1'
postgres-#     AND observation_date = '2016-05-11';
  id  | series |   realtime_start    |    realtime_end     |  observation_date   | value | row_indicator
------+--------+---------------------+---------------------+---------------------+-------+---------------
 6327 | GDPC1  | 2016-05-11 00:00:00 | 2016-05-11 00:00:00 | 2016-05-11 00:00:00 |   1.7 | expired
 6328 | GDPC1  | 2016-05-11 00:00:00 | 9999-12-31 00:00:00 | 2016-05-11 00:00:00 |   7.7 | current
(2 rows)

postgres=# SELECT *
postgres-# FROM fred.observation
postgres-#     WHERE series = 'GDPC1'
postgres-#     AND observation_date = '2016-05-11'
postgres-#     AND row_indicator = 'current';
  id  | series |   realtime_start    |    realtime_end     |  observation_date   | value | row_indicator
------+--------+---------------------+---------------------+---------------------+-------+---------------
 6328 | GDPC1  | 2016-05-11 00:00:00 | 9999-12-31 00:00:00 | 2016-05-11 00:00:00 |   7.7 | current
(1 row)
------



-- Get average rate of all series round to 3 decimal points
SELECT
  series,
  date_part('year', observation_date) AS YEAR,
  round(AVG(value),3) AS AVERAGE
FROM observation
--AND series = 'UNRATE'
GROUP BY series, date_part('year', observation_date)
ORDER BY series, year;

-- get all series than is greater than average unemployment rate

-- find duplicates

-- option 1
select series, observation_date
from observation
where row_indicator = 'current'
group by series, observation_date
having count(*) > 1 ;

-- option 2 (subquery)
select * from (
  SELECT id,
  ROW_NUMBER() OVER(PARTITION BY series, observation_date ORDER BY id asc) AS Row
  FROM observation
 where row_indicator = 'current'
) dups
where
dups.Row > 1


-- return recordw with no duplicates (distinct)

--option 1:
select count(*)
from
(
    select distinct series, observation_date
    from
    observation
) as t;

--option 2:
select count(*)
from
(select a.series,
    a.observation_date
  from observation a
  union
  select b.series,
    b.observation_date
  from observation b
  order by series) as t;


select
series,
value,
observation_date
from observation
where series = 'UNRATE'
and value >
(
    select
    avg(value)
    from observation
    where series = 'UNRATE'
)
order by observation_date;


-- unrate
select min(Value) from observation
where series = 'UNRATE';
--min: 2.5
select
avg(value)
from observation
where series = 'UNRATE'
--avg: 5.8206097560975610
select max(Value) from observation
where series = 'UNRATE';
--max: 10.8
select stddev(Value) from observation
where series = 'UNRATE';
-- 1.6421797119980836
select variance_pop(Value) from observation
where series = 'UNRATE';
-- 2.6967542064981089

create index observation_series_date_idx on observation (series, observation_date);
drop index observation_series_date_idx;


-- self join

select * from
observation a
join observation b on a.id = b.id
where a.id != b.id;

select * from
observation a, observation b
where a.id != b.id;

-- subquery
FROM()
WHERE col1 IN (), where EXISTS
SELECT col1, (), col3


select * from observation
where EXISTS(
    select * from observation_staging
)

--windows function

-- incremental aggregation
select series, observation_date, value,
sum(value) over (order by observation_date) as total,
count(value) over (order by observation_date) as count,
avg(value) over (order by observation_date) as average
from observation
WHERE observation_date BETWEEN '1980-01-01' AND '2015-12-31'
order by series, observation_date;

-- row number
select series, observation_date, value,
row_number() over (order by observation_date) as row_number,
rank() over (order by observation_date) as rank,
NTILE(100)  over (order by observation_date) as percentile
from observation
WHERE observation_date BETWEEN '1980-01-01' AND '2015-12-31'
order by series, observation_date;

select series, observation_date, value,
LAG(observation_date, 1) OVER
  (PARTITION BY observation_date ORDER BY observation_date) AS lag,
LEAD(observation_date, 1) OVER
  (PARTITION BY observation_date ORDER BY observation_date) AS lead
  from observation
  WHERE observation_date BETWEEN '1980-01-01' AND '2015-12-31'
  order by series, observation_date;



-- interview sql test
