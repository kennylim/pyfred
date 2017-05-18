 --DROP SCHEMA IF EXISTS fred CASCADE;

CREATE SCHEMA fred;


ALTER DATABASE postgres
SET search_path TO fred;


CREATE TABLE fred.observation_staging ( series varchar, realtime_start varchar, realtime_end varchar, observation_date varchar, value varchar, flag numeric, descriptions varchar);


CREATE TABLE fred.observation ( id SERIAL PRIMARY KEY, series varchar, realtime_start TIMESTAMP, realtime_end TIMESTAMP, observation_date TIMESTAMP, value numeric, row_indicator varchar);
