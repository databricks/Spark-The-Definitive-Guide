spark.sql("SELECT 1 + 1").show()


// COMMAND ----------

// in Scala
spark.read.json("/data/flight-data/json/2015-summary.json")
  .createOrReplaceTempView("some_sql_view") // DF => SQL

spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count)
FROM some_sql_view GROUP BY DEST_COUNTRY_NAME
""")
  .where("DEST_COUNTRY_NAME like 'S%'").where("`sum(count)` > 10")
  .count() // SQL => DF


// COMMAND ----------

val flights = spark.read.format("json")
  .load("/data/flight-data/json/2015-summary.json")
val just_usa_df = flights.where("dest_country_name = 'United States'")
just_usa_df.selectExpr("*").explain


// COMMAND ----------

def power3(number:Double):Double = number * number * number
spark.udf.register("power3", power3(_:Double):Double)


// COMMAND ----------

