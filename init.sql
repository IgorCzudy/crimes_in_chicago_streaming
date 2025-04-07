CREATE USER flink_user WITH PASSWORD 'flink_user';
GRANT ALL PRIVILEGES ON DATABASE crime_db TO flink_user;

-- CREATE DATABASE crime_db;

-- Connect to the crime_db database and create the tables
\c crime_db;

CREATE TABLE crime_per_district_day (
  total_crimes INT,
  domestic_crimes INT,
  district INT,
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  PRIMARY KEY (district, window_start)
);

CREATE TABLE crimes_anomaly (
  total_crimes INT,
  primary_type TEXT,
  district INT,
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  PRIMARY KEY (primary_type, district, window_start)
);

-- Optionally, delete data
-- DELETE FROM crime_per_district_day;
