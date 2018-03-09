DROP TABLE IF EXISTS sys_calendar;
CREATE TABLE sys_calendar
(
  calendar_date DATE NOT NULL PRIMARY KEY,
  year SMALLINT NOT NULL, -- 2000 to 2070
  month SMALLINT NOT NULL, -- 1 to 12
  day SMALLINT NOT NULL, -- 1 to 31
  quarter SMALLINT NOT NULL, -- 1 to 4
  day_of_calendar SMALLINT NOT NULL, -- 1 to 32,142
  day_of_week SMALLINT NOT NULL, -- 0 () to 6 ()
  day_of_year SMALLINT NOT NULL, -- 1 to 366
  days_in_month SMALLINT NOT NULL,
  dow_instance_in_month SMALLINT NOT NULL, -- 1 to 5
  dow_total_in_month SMALLINT NOT NULL, -- 4 to 5
  week_of_calendar SMALLINT NOT NULL,
  week_of_year SMALLINT NOT NULL, -- 1 to 53
  month_of_calendar SMALLINT NOT NULL,
  year_of_calendar SMALLINT NOT NULL,
  CONSTRAINT con_month CHECK (month >= 1 AND month <= 31),
  CONSTRAINT con_day_of_year CHECK (day_of_year >= 1 AND day_of_year <= 366), -- 366 allows for leap years
  CONSTRAINT con_week_of_year CHECK (week_of_year >= 1 AND week_of_year <= 53)
);
CREATE UNIQUE INDEX u_day_of_calendar ON sys_calendar (day_of_calendar);
CREATE UNIQUE INDEX u_calendar_date ON sys_calendar (calendar_date);


-- http://junctionbox.ca/2013/04/09/calendar-tables-for-postgresql-data-warehousing.html
INSERT INTO sys_calendar
(
    calendar_date
  , year
  , month
  , day
  , quarter
  , day_of_calendar
  , day_of_week
  , day_of_year
  , dow_instance_in_month
  , dow_total_in_month
  , week_of_calendar
  , week_of_year
  , month_of_calendar
  , year_of_calendar
  , days_in_month
)
(
  -- all this assumes the first date of the year (ie. 1 Jan) lands on a Sunday, Day 0.
  SELECT
    ts AS calendar_date
  , EXTRACT(YEAR FROM ts) AS year
  , EXTRACT(MONTH FROM ts) AS month
  , EXTRACT(DAY FROM ts) AS day
  , EXTRACT(QUARTER FROM ts) AS quarter
  , ROW_NUMBER() OVER(PARTITION BY 1 ORDER BY ts ASC) AS day_of_calendar
  , EXTRACT(DOW FROM ts) AS day_of_week
  , EXTRACT(DOY FROM ts) AS day_of_year
  , ROW_NUMBER() OVER(PARTITION BY EXTRACT(DOW FROM ts), concat(EXTRACT(YEAR FROM ts), LPAD(CAST(EXTRACT(MONTH FROM ts) AS TEXT), 2, '0'))::integer ORDER BY ts ASC) AS dow_instance_in_month
  , SUM(1) OVER(PARTITION BY EXTRACT(DOW FROM ts), concat(EXTRACT(YEAR FROM ts), LPAD(CAST(EXTRACT(MONTH FROM ts) AS TEXT), 2, '0'))::integer ORDER BY 1) AS dow_total_in_month
  , ROW_NUMBER() OVER(PARTITION BY EXTRACT(DOW FROM ts) ORDER BY ts ASC) AS week_of_calendar
  , EXTRACT(WEEK FROM ts) AS week_of_year --iso week
  , SUM(CASE WHEN EXTRACT(DAY FROM ts)= 1 THEN 1 END) OVER(PARTITION BY 1 ORDER BY ts ASC) AS month_of_calendar
  , SUM(CASE WHEN concat(LPAD(CAST(EXTRACT(MONTH FROM ts) AS TEXT), 2, '0'), LPAD(CAST(EXTRACT(DAY FROM ts) AS TEXT), 2, '0')) = '0101' THEN 1 END) OVER(PARTITION BY 1 ORDER BY ts ASC) AS year_of_calendar
  , MAX(EXTRACT(DAY FROM ts)) OVER(PARTITION BY concat(EXTRACT(YEAR FROM ts), LPAD(CAST(EXTRACT(MONTH FROM ts) AS TEXT), 2, '0'))::integer ORDER BY 1) AS days_in_month
  FROM generate_series('2012-01-01'::timestamp, '2099-12-31', '1day'::interval) AS t(ts) -- make sure the starting DATE is a SUNDAY
  --ORDER BY calendar_date ASc
);

--SELECT * FROM sys_calendar WHERE day_of_calendar = 1;
