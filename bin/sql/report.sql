
-- Get average rate of unemployment for each year starting with 1980 and going up to 2015
SELECT
  'UNRATE' AS series,
  date_part('year', observation_date) AS YEAR,
  AVG(value) AS AVERAGE
FROM observation
WHERE observation_date BETWEEN '1980-01-01' AND '2015-12-31'
AND series = 'UNRATE'
AND value IS NOT NULL
GROUP BY date_part('year', observation_date)
ORDER BY year;

-- improved version and rounded to 3 decimal points
SELECT
  series,
  date_part('year', observation_date) AS YEAR,
  round(AVG(value),3) AS AVERAGE
FROM observation
WHERE observation_date BETWEEN '1980-01-01' AND '2015-12-31'
AND series = 'UNRATE'
--AND value IS NOT NULL
GROUP BY series, date_part('year', observation_date)
ORDER BY year;
