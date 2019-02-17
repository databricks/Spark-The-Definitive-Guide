// 스칼라 버전
import spark.implicits._
case class Flight(DEST_COUNTRY_NAME: String,
                  ORIGIN_COUNTRY_NAME: String,
                  count: BigInt)
val flightsDF = spark.read
  .parquet("/data/flight-data/parquet/2010-summary.parquet/")
val flights = flightsDF.as[Flight]


// COMMAND ----------

// 스칼라 버전
flights
  .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
  .map(flight_row => flight_row)
  .take(5)

flights
  .take(5)
  .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
  .map(fr => Flight(fr.DEST_COUNTRY_NAME, fr.ORIGIN_COUNTRY_NAME, fr.count + 5))


// COMMAND ----------

// 스칼라 버전
val staticDataFrame = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/data/retail-data/by-day/*.csv")

staticDataFrame.createOrReplaceTempView("retail_data")
val staticSchema = staticDataFrame.schema


// COMMAND ----------

// 스칼라 버전
import org.apache.spark.sql.functions.{window, column, desc, col}
staticDataFrame
  .selectExpr(
    "CustomerId",
    "(UnitPrice * Quantity) as total_cost",
    "InvoiceDate")
  .groupBy(
    col("CustomerId"), window(col("InvoiceDate"), "1 day"))
  .sum("total_cost")
  .show(5)


// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "5")


// COMMAND ----------

val streamingDataFrame = spark.readStream
    .schema(staticSchema)
    .option("maxFilesPerTrigger", 1)
    .format("csv")
    .option("header", "true")
    .load("/data/retail-data/by-day/*.csv")


// COMMAND ----------

streamingDataFrame.isStreaming // returns true


// COMMAND ----------

// 스칼라 버전
val purchaseByCustomerPerHour = streamingDataFrame
  .selectExpr(
    "CustomerId",
    "(UnitPrice * Quantity) as total_cost",
    "InvoiceDate")
  .groupBy(
    col("CustomerId"), window(col("InvoiceDate"), "1 day"))
  .sum("total_cost")


// COMMAND ----------

// 스칼라 버전
purchaseByCustomerPerHour.writeStream
    .format("memory") // memory = store in-memory table
    .queryName("customer_purchases") // the name of the in-memory table
    .outputMode("complete") // complete = all the counts should be in the table
    .start()


// COMMAND ----------

// 스칼라 버전
spark.sql("""
  SELECT *
  FROM customer_purchases
  ORDER BY `sum(total_cost)` DESC
  """)
  .show(5)


// COMMAND ----------

staticDataFrame.printSchema()


// COMMAND ----------

// 스칼라 버전
import org.apache.spark.sql.functions.date_format
val preppedDataFrame = staticDataFrame
  .na.fill(0)
  .withColumn("day_of_week", date_format($"InvoiceDate", "EEEE"))
  .coalesce(5)


// COMMAND ----------

// 스칼라 버전
val trainDataFrame = preppedDataFrame
  .where("InvoiceDate < '2011-07-01'")
val testDataFrame = preppedDataFrame
  .where("InvoiceDate >= '2011-07-01'")


// COMMAND ----------

trainDataFrame.count()
testDataFrame.count()


// COMMAND ----------

// 스칼라 버전
import org.apache.spark.ml.feature.StringIndexer
val indexer = new StringIndexer()
  .setInputCol("day_of_week")
  .setOutputCol("day_of_week_index")


// COMMAND ----------

// 스칼라 버전
import org.apache.spark.ml.feature.OneHotEncoder
val encoder = new OneHotEncoder()
  .setInputCol("day_of_week_index")
  .setOutputCol("day_of_week_encoded")


// COMMAND ----------

// 스칼라 버전
import org.apache.spark.ml.feature.VectorAssembler

val vectorAssembler = new VectorAssembler()
  .setInputCols(Array("UnitPrice", "Quantity", "day_of_week_encoded"))
  .setOutputCol("features")


// COMMAND ----------

// 스칼라 버전
import org.apache.spark.ml.Pipeline

val transformationPipeline = new Pipeline()
  .setStages(Array(indexer, encoder, vectorAssembler))


// COMMAND ----------

// 스칼라 버전
val fittedPipeline = transformationPipeline.fit(trainDataFrame)


// COMMAND ----------

// 스칼라 버전
val transformedTraining = fittedPipeline.transform(trainDataFrame)


// COMMAND ----------

transformedTraining.cache()


// COMMAND ----------

// 스칼라 버전
import org.apache.spark.ml.clustering.KMeans
val kmeans = new KMeans()
  .setK(20)
  .setSeed(1L)


// COMMAND ----------

// 스칼라 버전
val kmModel = kmeans.fit(transformedTraining)


// COMMAND ----------

kmModel.computeCost(transformedTraining)


// COMMAND ----------

// 스칼라 버전
val transformedTest = fittedPipeline.transform(testDataFrame)


// COMMAND ----------

kmModel.computeCost(transformedTest)


// COMMAND ----------

// 스칼라 버전
spark.sparkContext.parallelize(Seq(1, 2, 3)).toDF()


// COMMAND ----------

