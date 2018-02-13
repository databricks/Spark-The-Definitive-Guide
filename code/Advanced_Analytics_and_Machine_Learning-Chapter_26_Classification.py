bInput = spark.read.format("parquet").load("/data/binary-classification")\
  .selectExpr("features", "cast(label as double) as label")


# COMMAND ----------

from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression()
print lr.explainParams() # see all parameters
lrModel = lr.fit(bInput)


# COMMAND ----------

print lrModel.coefficients
print lrModel.intercept


# COMMAND ----------

summary = lrModel.summary
print summary.areaUnderROC
summary.roc.show()
summary.pr.show()


# COMMAND ----------

summary.objectiveHistory


# COMMAND ----------

from pyspark.ml.classification import DecisionTreeClassifier
dt = DecisionTreeClassifier()
print dt.explainParams()
dtModel = dt.fit(bInput)


# COMMAND ----------

from pyspark.ml.classification import RandomForestClassifier
rfClassifier = RandomForestClassifier()
print rfClassifier.explainParams()
trainedModel = rfClassifier.fit(bInput)


# COMMAND ----------

from pyspark.ml.classification import GBTClassifier
gbtClassifier = GBTClassifier()
print gbtClassifier.explainParams()
trainedModel = gbtClassifier.fit(bInput)


# COMMAND ----------

from pyspark.ml.classification import NaiveBayes
nb = NaiveBayes()
print nb.explainParams()
trainedModel = nb.fit(bInput.where("label != 0"))


# COMMAND ----------

from pyspark.mllib.evaluation import BinaryClassificationMetrics
out = model.transform(bInput)\
  .select("prediction", "label")\
  .rdd.map(lambda x: (float(x[0]), float(x[1])))
metrics = BinaryClassificationMetrics(out)


# COMMAND ----------

print metrics.areaUnderPR
print metrics.areaUnderROC
print "Receiver Operating Characteristic"
metrics.roc.toDF().show()


# COMMAND ----------

