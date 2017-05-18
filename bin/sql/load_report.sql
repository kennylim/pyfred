-- load report

SELECT flag,
       descriptions,
       count(*)
FROM observation_staging
GROUP BY flag,
         descriptions
ORDER BY flag;
