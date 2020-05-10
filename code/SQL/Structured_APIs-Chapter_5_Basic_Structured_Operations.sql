SELECT * FROM dataFrameTable
SELECT columnName FROM dataFrameTable
SELECT columnName * 10, otherColumn, someOtherCol as c FROM dataFrameTable


-- COMMAND ----------

SELECT DEST_COUNTRY_NAME FROM dfTable LIMIT 2


-- COMMAND ----------

SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME FROM dfTable LIMIT 2


-- COMMAND ----------

SELECT DEST_COUNTRY_NAME as destination FROM dfTable LIMIT 2


-- COMMAND ----------

SELECT *, (DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry
FROM dfTable
LIMIT 2


-- COMMAND ----------

SELECT avg(count), count(distinct(DEST_COUNTRY_NAME)) FROM dfTable LIMIT 2


-- COMMAND ----------

SELECT *, 1 as One FROM dfTable LIMIT 2


-- COMMAND ----------

SELECT *, 1 as numberOne FROM dfTable LIMIT 2


-- COMMAND ----------

SELECT `This Long Column-Name`, `This Long Column-Name` as `new col`
FROM dfTableLong LIMIT 2


-- COMMAND ----------

set spark.sql.caseSensitive true


-- COMMAND ----------

SELECT *, cast(count as long) AS count2 FROM dfTable


-- COMMAND ----------

SELECT * FROM dfTable WHERE count < 2 LIMIT 2


-- COMMAND ----------

SELECT * FROM dfTable WHERE count < 2 AND ORIGIN_COUNTRY_NAME != "Croatia"
LIMIT 2


-- COMMAND ----------

SELECT COUNT(DISTINCT(ORIGIN_COUNTRY_NAME, DEST_COUNTRY_NAME)) FROM dfTable


-- COMMAND ----------

SELECT COUNT(DISTINCT ORIGIN_COUNTRY_NAME) FROM dfTable


-- COMMAND ----------

SELECT * FROM dfTable ORDER BY count DESC, DEST_COUNTRY_NAME ASC LIMIT 2


-- COMMAND ----------

SELECT * FROM dfTable LIMIT 6


-- COMMAND ----------

SELECT * FROM dfTable ORDER BY count desc LIMIT 6


-- COMMAND ----------

