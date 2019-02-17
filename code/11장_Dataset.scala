case class Flight(DEST_COUNTRY_NAME: String,
                  ORIGIN_COUNTRY_NAME: String, count: BigInt)


// COMMAND ----------

val flightsDF = spark.read
  .parquet("/data/flight-data/parquet/2010-summary.parquet/")
val flights = flightsDF.as[Flight]


// COMMAND ----------

flights.show(2)


// COMMAND ----------

flights.first.DEST_COUNTRY_NAME // United States


// COMMAND ----------

def originIsDestination(flight_row: Flight): Boolean = {
  return flight_row.ORIGIN_COUNTRY_NAME == flight_row.DEST_COUNTRY_NAME
}


// COMMAND ----------

flights.filter(flight_row => originIsDestination(flight_row)).first()


// COMMAND ----------

flights.collect().filter(flight_row => originIsDestination(flight_row))


// COMMAND ----------

val destinations = flights.map(f => f.DEST_COUNTRY_NAME)


// COMMAND ----------

val localDestinations = destinations.take(5)


// COMMAND ----------

case class FlightMetadata(count: BigInt, randomData: BigInt)

val flightsMeta = spark.range(500).map(x => (x, scala.util.Random.nextLong))
  .withColumnRenamed("_1", "count").withColumnRenamed("_2", "randomData")
  .as[FlightMetadata]


// COMMAND ----------

val flights2 = flights
  .joinWith(flightsMeta, flights.col("count") === flightsMeta.col("count"))


// COMMAND ----------

flights2.selectExpr("_1.DEST_COUNTRY_NAME")


// COMMAND ----------

flights2.take(2)


// COMMAND ----------

val flights2 = flights.join(flightsMeta, Seq("count"))


// COMMAND ----------

val flights2 = flights.join(flightsMeta.toDF(), Seq("count"))


// COMMAND ----------

flights.groupBy("DEST_COUNTRY_NAME").count()


// COMMAND ----------

flights.groupByKey(x => x.DEST_COUNTRY_NAME).count()


// COMMAND ----------

flights.groupByKey(x => x.DEST_COUNTRY_NAME).count().explain


// COMMAND ----------

def grpSum(countryName:String, values: Iterator[Flight]) = {
  values.dropWhile(_.count < 5).map(x => (countryName, x))
}
flights.groupByKey(x => x.DEST_COUNTRY_NAME).flatMapGroups(grpSum).show(5)


// COMMAND ----------

def grpSum2(f:Flight):Integer = {
  1
}
flights.groupByKey(x => x.DEST_COUNTRY_NAME).mapValues(grpSum2).count().take(5)


// COMMAND ----------

def sum2(left:Flight, right:Flight) = {
  Flight(left.DEST_COUNTRY_NAME, null, left.count + right.count)
}
flights.groupByKey(x => x.DEST_COUNTRY_NAME).reduceGroups((l, r) => sum2(l, r))
  .take(5)


// COMMAND ----------

flights.groupBy("DEST_COUNTRY_NAME").count().explain


// COMMAND ----------

