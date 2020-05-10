# %%
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("spark-book").getOrCreate()

# %%
spark

# %%
staticDataFrame = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("../data/retail-data/by-day/*.csv")
)

staticDataFrame.createOrReplaceTempView("retail_data")
staticSchema = staticDataFrame.schema

# %%
from pyspark.sql.functions import window, column, desc, col

(staticDataFrame
    .selectExpr(
    "CustomerId", "(UnitPrice * Quantity) as total_cost", "InvoiceDate")
    .groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 day"))
    .sum("total_cost")
    .show(5)
)

# %%
streamingDataFrame = (
    spark.readStream
    .schema(staticSchema)
    .option("maxFilesPerTrigger", 1)
    .format("csv")
    .option("header", "true")\
    .load("/data/retail-data/by-day/*.csv")
)

# %%
purchaseByCustomerPerHour = (
    streamingDataFrame
    .selectExpr("CustomerId", "(UnitPrice * Quantity) as total_cost", "InvoiceDate")
    .groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 day"))
    .sum("total_cost")
)

# %%
purchaseByCustomerPerHour.writeStream\
    .format("memory")\
    .queryName("customer_purchases")\
    .outputMode("complete")\
    .start()

# %%
spark.sql("""
  SELECT *
  FROM customer_purchases
  ORDER BY `sum(total_cost)` DESC
  """)\
  .show(5)

# %%
from pyspark.sql.functions import date_format, col
preppedDataFrame = (
    staticDataFrame
    .na.fill(0)
    .withColumn("day_of_week", date_format(col("InvoiceDate"), "EEEE"))
    .coalesce(5)
)

# %%
trainDataFrame = (
    preppedDataFrame
    .where("InvoiceDate < '2011-07-01'")
)

testDataFrame = (
    preppedDataFrame
    .where("InvoiceDate >= '2011-07-01'")
)

# %%
from pyspark.ml.feature import StringIndexer
indexer = (
    StringIndexer()
    .setInputCol("day_of_week")
    .setOutputCol("day_of_week_index")
)

# %%

from pyspark.ml.feature import OneHotEncoder
encoder = (
    OneHotEncoder()
    .setInputCol("day_of_week_index")
    .setOutputCol("day_of_week_encoded")
)

# %%
from pyspark.ml.feature import VectorAssembler

vectorAssembler = (
    VectorAssembler()
    .setInputCols(["UnitPrice", "Quantity", "day_of_week_encoded"])
    .setOutputCol("features")
)

# %%
from pyspark.ml import Pipeline

transformationPipeline = (
    Pipeline()
    .setStages([indexer, encoder, vectorAssembler])
)

# %%
fittedPipeline = transformationPipeline.fit(trainDataFrame)

# %%
transformedTraining = fittedPipeline.transform(trainDataFrame)

# %%
from pyspark.ml.clustering import KMeans
kmeans = KMeans()\
  .setK(20)\
  .setSeed(1)

# %%
kmModel = kmeans.fit(transformedTraining)

# %%
transformedTest = fittedPipeline.transform(testDataFrame)

# %%
from pyspark.sql import Row

spark.sparkContext.parallelize([Row(1), Row(2), Row(3)]).toDF()

# %%
