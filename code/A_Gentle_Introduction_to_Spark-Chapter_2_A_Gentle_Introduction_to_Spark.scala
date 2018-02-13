spark


// COMMAND ----------

// in Scala
val myRange = spark.range(1000).toDF("number")


// COMMAND ----------

// in Scala
val divisBy2 = myRange.where("number % 2 = 0")


// COMMAND ----------

divisBy2.count()


// COMMAND ----------

// in Scala
val flightData2015 = spark
  .read
  .option("inferSchema", "true")
  .option("header", "true")
  .csv("/data/flight-data/csv/2015-summary.csv")


// COMMAND ----------

flightData2015.take(3)


// COMMAND ----------

flightData2015.sort("count").explain()


// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "5")


// COMMAND ----------

flightData2015.sort("count").take(2)


// COMMAND ----------

flightData2015.createOrReplaceTempView("flight_data_2015")


// COMMAND ----------

// in Scala
val sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
""")

val dataFrameWay = flightData2015
  .groupBy('DEST_COUNTRY_NAME)
  .count()

sqlWay.explain
dataFrameWay.explain


// COMMAND ----------

spark.sql("SELECT max(count) from flight_data_2015").take(1)


// COMMAND ----------

// in Scala
import org.apache.spark.sql.functions.max

flightData2015.select(max("count")).take(1)


// COMMAND ----------

// in Scala
val maxSql = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""")

maxSql.show()


// COMMAND ----------

// in Scala
import org.apache.spark.sql.functions.desc

flightData2015
  .groupBy("DEST_COUNTRY_NAME")
  .sum("count")
  .withColumnRenamed("sum(count)", "destination_total")
  .sort(desc("destination_total"))
  .limit(5)
  .show()


// COMMAND ----------

// in Scala
flightData2015
  .groupBy("DEST_COUNTRY_NAME")
  .sum("count")
  .withColumnRenamed("sum(count)", "destination_total")
  .sort(desc("destination_total"))
  .limit(5)
  .explain()


// COMMAND ----------

