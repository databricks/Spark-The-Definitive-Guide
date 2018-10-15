// 스칼라 버전
val df = spark.range(500).toDF("number")
df.select(df.col("number") + 10)


// COMMAND ----------

// 스칼라 버전
spark.range(2).toDF().collect()


// COMMAND ----------

import org.apache.spark.sql.types._
val b = ByteType


// COMMAND ----------

