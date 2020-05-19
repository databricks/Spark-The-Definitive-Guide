import pandas as pd
df = pd.DataFrame({"first":list(range(200)), "second":list(range(50,250))})


# COMMAND ----------

sparkDF = spark.createDataFrame(df)


# COMMAND ----------

newPDF = sparkDF.toPandas()
newPDF.head()


# COMMAND ----------

