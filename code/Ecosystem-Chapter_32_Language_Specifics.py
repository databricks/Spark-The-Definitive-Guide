import pandas as pd
df = pd.DataFrame({"first":range(200), "second":range(50,250)})


# COMMAND ----------

sparkDF = spark.createDataFrame(df)


# COMMAND ----------

newPDF = sparkDF.toPandas()
newPDF.head()


# COMMAND ----------

