SELECT COUNT(*) FROM dfTable


-- COMMAND ----------

SELECT COUNT(DISTINCT *) FROM DFTABLE


-- COMMAND ----------

SELECT approx_count_distinct(StockCode, 0.1) FROM DFTABLE


-- COMMAND ----------

SELECT first(StockCode), last(StockCode) FROM dfTable


-- COMMAND ----------

SELECT min(Quantity), max(Quantity) FROM dfTable


-- COMMAND ----------

SELECT sum(Quantity) FROM dfTable


-- COMMAND ----------

SELECT SUM(Quantity) FROM dfTable -- 29310


-- COMMAND ----------

SELECT var_pop(Quantity), var_samp(Quantity),
  stddev_pop(Quantity), stddev_samp(Quantity)
FROM dfTable


-- COMMAND ----------

SELECT skewness(Quantity), kurtosis(Quantity) FROM dfTable


-- COMMAND ----------

SELECT corr(InvoiceNo, Quantity), covar_samp(InvoiceNo, Quantity),
  covar_pop(InvoiceNo, Quantity)
FROM dfTable


-- COMMAND ----------

SELECT collect_set(Country), collect_set(Country) FROM dfTable


-- COMMAND ----------

SELECT count(*) FROM dfTable GROUP BY InvoiceNo, CustomerId


-- COMMAND ----------

SELECT avg(Quantity), stddev_pop(Quantity), InvoiceNo FROM dfTable
GROUP BY InvoiceNo


-- COMMAND ----------

SELECT CustomerId, date, Quantity,
  rank(Quantity) OVER (PARTITION BY CustomerId, date
                       ORDER BY Quantity DESC NULLS LAST
                       ROWS BETWEEN
                         UNBOUNDED PRECEDING AND
                         CURRENT ROW) as rank,

  dense_rank(Quantity) OVER (PARTITION BY CustomerId, date
                             ORDER BY Quantity DESC NULLS LAST
                             ROWS BETWEEN
                               UNBOUNDED PRECEDING AND
                               CURRENT ROW) as dRank,

  max(Quantity) OVER (PARTITION BY CustomerId, date
                      ORDER BY Quantity DESC NULLS LAST
                      ROWS BETWEEN
                        UNBOUNDED PRECEDING AND
                        CURRENT ROW) as maxPurchase
FROM dfWithDate WHERE CustomerId IS NOT NULL ORDER BY CustomerId


-- COMMAND ----------

SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
GROUP BY customerId, stockCode
ORDER BY CustomerId DESC, stockCode DESC


-- COMMAND ----------

SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
GROUP BY customerId, stockCode GROUPING SETS((customerId, stockCode))
ORDER BY CustomerId DESC, stockCode DESC


-- COMMAND ----------

SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
GROUP BY customerId, stockCode GROUPING SETS((customerId, stockCode),())
ORDER BY CustomerId DESC, stockCode DESC


-- COMMAND ----------

