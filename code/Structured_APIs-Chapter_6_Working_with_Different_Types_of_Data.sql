SELECT 5, "five", 5.0


-- COMMAND ----------

SELECT * FROM dfTable WHERE StockCode in ("DOT") AND(UnitPrice > 600 OR
    instr(Description, "POSTAGE") >= 1)


-- COMMAND ----------

SELECT UnitPrice, (StockCode = 'DOT' AND
  (UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1)) as isExpensive
FROM dfTable
WHERE (StockCode = 'DOT' AND
       (UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1))


-- COMMAND ----------

SELECT customerId, (POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity
FROM dfTable


-- COMMAND ----------

SELECT round(2.5), bround(2.5)


-- COMMAND ----------

SELECT corr(Quantity, UnitPrice) FROM dfTable


-- COMMAND ----------

SELECT initcap(Description) FROM dfTable


-- COMMAND ----------

SELECT Description, lower(Description), Upper(lower(Description)) FROM dfTable


-- COMMAND ----------

SELECT
  ltrim('    HELLLOOOO  '),
  rtrim('    HELLLOOOO  '),
  trim('    HELLLOOOO  '),
  lpad('HELLOOOO  ', 3, ' '),
  rpad('HELLOOOO  ', 10, ' ')
FROM dfTable


-- COMMAND ----------

SELECT
  regexp_replace(Description, 'BLACK|WHITE|RED|GREEN|BLUE', 'COLOR') as
  color_clean, Description
FROM dfTable


-- COMMAND ----------

SELECT translate(Description, 'LEET', '1337'), Description FROM dfTable


-- COMMAND ----------

SELECT regexp_extract(Description, '(BLACK|WHITE|RED|GREEN|BLUE)', 1),
  Description
FROM dfTable


-- COMMAND ----------

SELECT Description FROM dfTable
WHERE instr(Description, 'BLACK') >= 1 OR instr(Description, 'WHITE') >= 1


-- COMMAND ----------

SELECT date_sub(today, 5), date_add(today, 5) FROM dateTable


-- COMMAND ----------

SELECT to_date('2016-01-01'), months_between('2016-01-01', '2017-01-01'),
datediff('2016-01-01', '2017-01-01')
FROM dateTable


-- COMMAND ----------

SELECT to_date(date, 'yyyy-dd-MM'), to_date(date2, 'yyyy-dd-MM'), to_date(date)
FROM dateTable2


-- COMMAND ----------

SELECT to_timestamp(date, 'yyyy-dd-MM'), to_timestamp(date2, 'yyyy-dd-MM')
FROM dateTable2


-- COMMAND ----------

SELECT cast(to_date("2017-01-01", "yyyy-dd-MM") as timestamp)


-- COMMAND ----------

SELECT
  ifnull(null, 'return_value'),
  nullif('value', 'value'),
  nvl(null, 'return_value'),
  nvl2('not_null', 'return_value', "else_value")
FROM dfTable LIMIT 1


-- COMMAND ----------

SELECT * FROM dfTable WHERE Description IS NOT NULL


-- COMMAND ----------

SELECT complex.* FROM complexDF


-- COMMAND ----------

SELECT split(Description, ' ') FROM dfTable


-- COMMAND ----------

SELECT split(Description, ' ')[0] FROM dfTable


-- COMMAND ----------

SELECT array_contains(split(Description, ' '), 'WHITE') FROM dfTable


-- COMMAND ----------

SELECT Description, InvoiceNo, exploded
FROM (SELECT *, split(Description, " ") as splitted FROM dfTable)
LATERAL VIEW explode(splitted) as exploded


-- COMMAND ----------

SELECT map(Description, InvoiceNo) as complex_map FROM dfTable
WHERE Description IS NOT NULL


-- COMMAND ----------

SELECT power3(12), power3py(12) -- doesn't work because of return type


-- COMMAND ----------

## Hive UDFs


-- COMMAND ----------

CREATE TEMPORARY FUNCTION myFunc AS 'com.organization.hive.udf.FunctionName'


-- COMMAND ----------

