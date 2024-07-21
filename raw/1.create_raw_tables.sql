-- Databricks notebook source
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create tables from CSV files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
    circuitId INT,
    circuitRef STRING,
    name STRING,
    location STRING,
    country STRING,
    lat DOUBLE,
    lng DOUBLE,
    alt INT,
    url STRING
)
USING csv
OPTIONS (path "/mnt/formula1dlld/raw/circuits.csv", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
    raceId INT,
    year INT,
    round INT,
    circuitId INT,
    name STRING,
    date DATE,
    time STRING,
    url STRING
)
USING csv
OPTIONS (path "/mnt/formula1dlld/raw/races.csv", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Create tables for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create constructors table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
    constructorId INT, 
    constructorRef STRING, 
    name STRING, 
    nationality STRING, 
    url STRING
)
USING json
OPTIONS (path "/mnt/formula1dlld/raw/constructors.json", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create drivers table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
    driverId INT,
    driverRef STRING,
    number INT,
    code STRING,
    name STRUCT<forename: STRING, surname: STRING>,
    dob Date,
    nationality STRING,
    url STRING
)
USING json
OPTIONS (path "/mnt/formula1dlld/raw/drivers.json", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create results table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
    constructorId INT,
    driverId INT,
    fastestLap INT,
    fastestLapSpeed STRING,
    fastestLapTime  STRING,
    grid INT,
    laps INT,
    milliseconds INT, 
    number INT,
    points FLOAT,
    position INT,
    positionOrder INT,
    positionText STRING,
    raceId INT,
    rank INT,
    resultId INT,
    statusId INT,
    time STRING
)
USING json
OPTIONS (path "/mnt/formula1dlld/raw/results.json", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create pit stops table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
    raceId Integer,
    driverId Integer,
    stop String,
    lap Integer,
    time  String,
    duration String,
    milliseconds Integer
)
USING json
OPTIONS (path "/mnt/formula1dlld/raw/pit_stops.json", header true, multiLine true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create lap times table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
    raceId INT,
    driverId INT,
    lap INT,
    position INT,
    time  STRING,
    milliseconds INT
)
USING csv
OPTIONS (path "/mnt/formula1dlld/raw/lap_times", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create qualifying table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
    qualifyId INT,
    raceId INT,
    driverId INT,
    constructorId INT,
    number INT,
    position INT,
    q1  STRING,
    q2  STRING,
    q3  STRING
)
USING json
OPTIONS (path "/mnt/formula1dlld/raw/qualifying", header true, multiLine true)

-- COMMAND ----------

DESC EXTENDED f1_raw.qualifying

-- COMMAND ----------

-- MAGIC %md
