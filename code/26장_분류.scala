// 스칼라 버전
val bInput = spark.read.format("parquet").load("/data/binary-classification")
  .selectExpr("features", "cast(label as double) as label")


// COMMAND ----------

// 스칼라 버전
import org.apache.spark.ml.classification.LogisticRegression
val lr = new LogisticRegression()
println(lr.explainParams()) // see all parameters
val lrModel = lr.fit(bInput)


// COMMAND ----------

// 스칼라 버전
println(lrModel.coefficients)
println(lrModel.intercept)


// COMMAND ----------

// 스칼라 버전
import org.apache.spark.ml.classification.BinaryLogisticRegressionSummary
val summary = lrModel.summary
val bSummary = summary.asInstanceOf[BinaryLogisticRegressionSummary]
println(bSummary.areaUnderROC)
bSummary.roc.show()
bSummary.pr.show()


// COMMAND ----------

// 스칼라 버전
import org.apache.spark.ml.classification.DecisionTreeClassifier
val dt = new DecisionTreeClassifier()
println(dt.explainParams())
val dtModel = dt.fit(bInput)


// COMMAND ----------

// 스칼라 버전
import org.apache.spark.ml.classification.RandomForestClassifier
val rfClassifier = new RandomForestClassifier()
println(rfClassifier.explainParams())
val trainedModel = rfClassifier.fit(bInput)


// COMMAND ----------

// 스칼라 버전
import org.apache.spark.ml.classification.GBTClassifier
val gbtClassifier = new GBTClassifier()
println(gbtClassifier.explainParams())
val trainedModel = gbtClassifier.fit(bInput)


// COMMAND ----------

// 스칼라 버전
import org.apache.spark.ml.classification.NaiveBayes
val nb = new NaiveBayes()
println(nb.explainParams())
val trainedModel = nb.fit(bInput.where("label != 0"))


// COMMAND ----------

// 스칼라 버전
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
val out = trainedModel.transform(bInput)
  .select("prediction", "label")
  .rdd.map(x => (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double]))
val metrics = new BinaryClassificationMetrics(out)


// COMMAND ----------

// 스칼라 버전
metrics.areaUnderPR
metrics.areaUnderROC
println("Receiver Operating Characteristic")
metrics.roc.toDF().show()


// COMMAND ----------

