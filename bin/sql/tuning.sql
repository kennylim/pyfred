
UPDATE observation_staging
SET flag = 1,
    descriptions = 'insert new record'
FROM
    (SELECT s.series,
            s.observation_date ::TIMESTAMP ::date
     FROM fred.observation_staging s
     LEFT  JOIN fred.observation o ON s.series = o.series
     AND s.observation_date ::TIMESTAMP ::date = o.observation_date
     WHERE o.series IS NULL
         AND o.observation_date IS NULL) AS o
WHERE observation_staging.series = o.series
    AND observation_staging.observation_date ::TIMESTAMP ::date = o.observation_date;

 -- cost is more 160 opposed to using minus
 /*
    ────────────────────────────────────────────────────────────────────────────────┐
    │                                   QUERY PLAN                                   │
    ├────────────────────────────────────────────────────────────────────────────────┤
    │ Update on observation_staging  (cost=160.60..180.83 rows=193 width=64)         │
    │   ->  Merge Join  (cost=160.60..180.83 rows=193 width=64)                      │
    │         Merge Cond: (((s.series)::text = (observation_staging.series)::text) A…│
    │…ND (((s.observation_date)::timestamp without time zone)::date = (((observation…│
    │…_staging.observation_date)::timestamp without time zone)::date)))              │
    │         ->  Merge Anti Join  (cost=140.53..149.80 rows=139 width=29)           │
    │               Merge Cond: (((s.series)::text = (o.series)::text) AND ((((s.obs…│
    │…ervation_date)::timestamp without time zone)::date) = o.observation_date))     │
    │               ->  Sort  (cost=20.07..20.76 rows=278 width=23)                  │
    │                     Sort Key: s.series, (((s.observation_date)::timestamp with…│
    │…out time zone)::date)                                                          │
    │                     ->  Seq Scan on observation_staging s  (cost=0.00..8.78 ro…│
    │…ws=278 width=23)                                                               │
    │               ->  Sort  (cost=120.47..124.58 rows=1647 width=21)               │
    │                     Sort Key: o.series, o.observation_date                     │
    │                     ->  Seq Scan on observation o  (cost=0.00..32.47 rows=1647…│
    │… width=21)                                                                     │
    │         ->  Sort  (cost=20.07..20.76 rows=278 width=52)                        │
    │               Sort Key: observation_staging.series, (((observation_staging.obs…│
    │…ervation_date)::timestamp without time zone)::date)                            │
    │               ->  Seq Scan on observation_staging  (cost=0.00..8.78 rows=278 w…│
    │…idth=52)                                                                       │
    └────────────────────────────────────────────────────────────────────────────────┘
    (14 rows)

 --   minus cost is less at 103

 ┌────────────────────────────────────────────────────────────────────────────────┐
│                                   QUERY PLAN                                   │
├────────────────────────────────────────────────────────────────────────────────┤
│ Update on observation_staging  (cost=103.56..114.70 rows=2 width=116)          │
│   ->  Merge Join  (cost=103.56..114.70 rows=2 width=116)                       │
│         Merge Cond: (((o.series)::text = (observation_staging.series)::text) A…│
│…ND (o.observation_date = (((observation_staging.observation_date)::timestamp w…│
│…ithout time zone)::date)))                                                     │
│         ->  Subquery Scan on o  (cost=83.50..86.97 rows=278 width=104)         │
│               ->  Sort  (cost=83.50..84.19 rows=278 width=15)                  │
│                     Sort Key: "*
SELECT* 1".series, (("*
SELECT* 1".observation_…│
│…date)::timestamp without time zone)                                            │
│                     ->  HashSetOp Except  (cost=0.00..72.21 rows=278 width=15) │
│                           ->  Append  (cost=0.00..62.59 rows=1925 width=15)    │
│                                 ->  Subquery Scan on "*
SELECT* 1"  (cost=0.00.…│
│….13.65 rows=278 width=17)                                                      │
│                                       ->  Seq Scan on observation_staging obse…│
│…rvation_staging_1  (cost=0.00..10.87 rows=278 width=17)                        │
│                                 ->  Subquery Scan on "*
SELECT* 2"  (cost=0.00.…│
│….48.94 rows=1647 width=15)                                                     │
│                                       ->  Seq Scan on observation  (cost=0.00.…│
│….32.47 rows=1647 width=15)                                                     │
│         ->  Sort  (cost=20.07..20.76 rows=278 width=52)                        │
│               Sort Key: observation_staging.series, (((observation_staging.obs…│
│…ervation_date)::timestamp without time zone)::date)                            │
│               ->  Seq Scan on observation_staging  (cost=0.00..8.78 rows=278 w…│
│…idth=52)                                                                       │
└────────────────────────────────────────────────────────────────────────────────┘
(15 rows)

*/
