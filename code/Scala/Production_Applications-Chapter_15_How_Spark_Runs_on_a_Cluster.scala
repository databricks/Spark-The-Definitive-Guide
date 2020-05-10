// Creating a SparkSession in Scala
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().appName("Databricks Spark Example")
  .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
  .getOrCreate()


// COMMAND ----------

// in Scala
import org.apache.spark.SparkContext
val sc = SparkContext.getOrCreate()


// COMMAND ----------

step4.explain()


// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 50)


// COMMAND ----------

