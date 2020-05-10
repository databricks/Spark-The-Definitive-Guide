// This Java code was contributed by @neeleshkumar-mannur

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.LogisticRegressionTrainingSummary;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.RFormula;
import org.apache.spark.ml.feature.RFormulaModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Advanced_Analytics_and_Machine_Learning_Chapter_24_Advanced_Analytics_and_Machine_Learning {

	public static void main(String[] args) throws IOException {
		SparkSession spark = SparkSession
				.builder()
				.master("local[*]")
				.appName("Chapter24AdvancedAnalyticsAndMachineLearning")
				.getOrCreate();

		// Creating a Dense Vector
		Vector denseVector = Vectors.dense(1.0, 2.0, 3.0);
		System.out.println("Dense Vector:" + denseVector.toString());

		// Creating a Sparse Vector
		int size = 3;
		int[] idx = new int[] { 1, 2 };
		double[] values = new double[] { 2.0, 3.0 };

		Vector sparseVector = Vectors.sparse(size, idx, values);
		System.out.println("Sparse Vector:" + sparseVector.toString());

		// Reading the simple json file into a Dataframe / Dataset
		Dataset<Row> df = spark.read().json("data/simple-ml");
		df.orderBy("value2").show();

		// Initializing RFormula
		RFormula supervised = new RFormula().setFormula("lab ~ . + color:value1 + color:value2");

		// Applying RFormula
		RFormulaModel fittedRF = supervised.fit(df);
		Dataset<Row> preparedDF = fittedRF.transform(df);
		preparedDF.show();

		// Splitting PreparedDF into Train DF and Test DF
		Dataset<Row>[] trainTestDataArray = preparedDF.randomSplit(new double[] { 0.7, 0.3 });
		Dataset<Row> train = trainTestDataArray[0];
		Dataset<Row> test = trainTestDataArray[1];

		// Displaying sample data from both train as well as test dataset
		train.show();
		test.show();
		
		// Applying Logistic Regression on the split data
		LogisticRegression logisticRegression = new LogisticRegression().setLabelCol("label")
				.setFeaturesCol("features");

		// Printing the params
		System.out.println(logisticRegression.explainParams());

		// Fitting Train dataset into the model
		LogisticRegressionModel fittedLR = logisticRegression.fit(train);
		fittedLR.transform(train).select("label","prediction").show();
		
		// Splitting the original dataset into train and test set
		Dataset<Row> [] trainTestSet = df.randomSplit(new double [] {0.7, 0.3});
		train = trainTestSet[0];
		test = trainTestSet[1];
		
		// Initializing RFormula
		RFormula rForm = new RFormula();

		// Reinitializing the Logistic Regression model since it is already trained on other data
		logisticRegression = new LogisticRegression().setLabelCol("label").setFeaturesCol("features");
		
		// Creating a Pipeline for execution
		PipelineStage[] stages = new PipelineStage[2];
		stages[0] = rForm;
		stages[1] = logisticRegression;

		Pipeline pipeline = new Pipeline().setStages(stages);

		// Applying Param Grid Builder
		List<String> paramValues = new ArrayList<String>();
		paramValues.add("lab ~ . + color:value1");
		paramValues.add("lab ~ . + color:value1 + color:value2");

		ParamMap [] params = new ParamGridBuilder()
				.addGrid(rForm.formula(), scala.collection.JavaConverters
						.asScalaIteratorConverter(paramValues.iterator()).asScala().toSeq())
				.addGrid(logisticRegression.elasticNetParam(), new double [] {0.0, 0.5, 1.0})
				.addGrid(logisticRegression.regParam(), new double [] {0.1, 2.0})
				.build();
		
		// Obtaining the Evaluator
		BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator()
				.setMetricName("areaUnderROC")
				.setRawPredictionCol("prediction")
				.setLabelCol("label");
		
		// Creating Train Validation Split
		TrainValidationSplit trainValidationSplit = new TrainValidationSplit()
				.setTrainRatio(0.75)
				.setEstimatorParamMaps(params)
				.setEstimator(pipeline)
				.setEvaluator(evaluator);
		
		TrainValidationSplitModel tvsFitted = trainValidationSplit.fit(train);
		
		evaluator.evaluate(tvsFitted.transform(test));
		
		// Getting the best Model
		PipelineModel trainedPipeline = (PipelineModel) tvsFitted.bestModel();
		LogisticRegressionModel trainedLR = (LogisticRegressionModel) trainedPipeline.stages()[1];
		LogisticRegressionTrainingSummary summaryLR = trainedLR.summary();
		
		for(double objectiveHist: summaryLR.objectiveHistory()) {
			System.out.println(objectiveHist);
		}
		
		// Persisting the model to disk
		tvsFitted.write().overwrite().save("tmp/modelLocation");
		
		// Loading the persisted model from disk and testing
		TrainValidationSplitModel model = TrainValidationSplitModel.load("tmp/modelLocation");
		Dataset<Row> tested = model.transform(test);
		
		tested.show();
	}
}
