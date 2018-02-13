df = spark.range(500).toDF("number")
df.select(df["number"] + 10)


# COMMAND ----------

spark.range(2).collect()


# COMMAND ----------

from pyspark.sql.types import *
b = ByteType()


# COMMAND ----------

