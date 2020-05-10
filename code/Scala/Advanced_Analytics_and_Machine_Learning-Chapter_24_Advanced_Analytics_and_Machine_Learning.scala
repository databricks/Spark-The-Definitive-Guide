// in Scala
import org.apache.spark.ml.linalg.Vectors
val denseVec = Vectors.dense(1.0, 2.0, 3.0)
val size = 3
val idx = Array(1,2) // locations of non-zero elements in vector
val values = Array(2.0,3.0)
val sparseVec = Vectors.sparse(size, idx, values)
sparseVec.toDense
denseVec.toSparse


// COMMAND ----------

// in Scala
var df = spark.read.json("/data/simple-ml")
df.orderBy("value2").show()


// COMMAND ----------

spark.read.format("libsvm").load(
  "/data/sample_libsvm_data.txt")


// COMMAND ----------

// in Scala
import org.apache.spark.ml.feature.RFormula
val supervised = new RFormula()
  .setFormula("lab ~ . + color:value1 + color:value2")


// COMMAND ----------

// in Scala
val fittedRF = supervised.fit(df)
val preparedDF = fittedRF.transform(df)
preparedDF.show()


// COMMAND ----------

// in Scala
val Array(train, test) = preparedDF.randomSplit(Array(0.7, 0.3))


// COMMAND ----------

// in Scala
import org.apache.spark.ml.classification.LogisticRegression
val lr = new LogisticRegression().setLabelCol("label").setFeaturesCol("features")


// COMMAND ----------

// in Scala
println(lr.explainParams())


// COMMAND ----------

// in Scala
val fittedLR = lr.fit(train)


// COMMAND ----------

fittedLR.transform(train).select("label", "prediction").show()


// COMMAND ----------

// in Scala
val Array(train, test) = df.randomSplit(Array(0.7, 0.3))


// COMMAND ----------

// in Scala
val rForm = new RFormula()
val lr = new LogisticRegression().setLabelCol("label").setFeaturesCol("features")


// COMMAND ----------

// in Scala
import org.apache.spark.ml.Pipeline
val stages = Array(rForm, lr)
val pipeline = new Pipeline().setStages(stages)


// COMMAND ----------

// in Scala
import org.apache.spark.ml.tuning.ParamGridBuilder
val params = new ParamGridBuilder()
  .addGrid(rForm.formula, Array(
    "lab ~ . + color:value1",
    "lab ~ . + color:value1 + color:value2"))
  .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
  .addGrid(lr.regParam, Array(0.1, 2.0))
  .build()


// COMMAND ----------

// in Scala
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
val evaluator = new BinaryClassificationEvaluator()
  .setMetricName("areaUnderROC")
  .setRawPredictionCol("prediction")
  .setLabelCol("label")


// COMMAND ----------

// in Scala
import org.apache.spark.ml.tuning.TrainValidationSplit
val tvs = new TrainValidationSplit()
  .setTrainRatio(0.75) // also the default.
  .setEstimatorParamMaps(params)
  .setEstimator(pipeline)
  .setEvaluator(evaluator)


// COMMAND ----------

// in Scala
val tvsFitted = tvs.fit(train)


// COMMAND ----------

evaluator.evaluate(tvsFitted.transform(test)) // 0.9166666666666667


// COMMAND ----------

// in Scala
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.LogisticRegressionModel
val trainedPipeline = tvsFitted.bestModel.asInstanceOf[PipelineModel]
val TrainedLR = trainedPipeline.stages(1).asInstanceOf[LogisticRegressionModel]
val summaryLR = TrainedLR.summary
summaryLR.objectiveHistory // 0.6751425885789243, 0.5543659647777687, 0.473776...


// COMMAND ----------

tvsFitted.write.overwrite().save("/tmp/modelLocation")


// COMMAND ----------

// in Scala
import org.apache.spark.ml.tuning.TrainValidationSplitModel
val model = TrainValidationSplitModel.load("/tmp/modelLocation")
model.transform(test)


// COMMAND ----------

