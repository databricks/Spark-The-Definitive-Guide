# databricks 런타임 환경에서 테스트 하기 위해서는 아래의 코드를 먼저 실행해야 합니다.
%sh
curl -O http://download.tensorflow.org/example_images/flower_photos.tgz
tar xzf flower_photos.tgz &>/dev/null


# COMMAND ----------
dbutils.fs.ls('file:/databricks/driver/flower_photos')


# COMMAND ----------

img_dir = '/tmp/flower_photos'
dbutils.fs.mkdirs(img_dir)

dbutils.fs.cp('file:/databricks/driver/flower_photos/tulips', img_dir + "/tulips", recurse=True)
dbutils.fs.cp('file:/databricks/driver/flower_photos/daisy', img_dir + "/daisy", recurse=True)
dbutils.fs.cp('file:/databricks/driver/flower_photos/LICENSE.txt', img_dir)


# COMMAND ----------

sample_img_dir = img_dir + "/sample"
dbutils.fs.rm(sample_img_dir, recurse=True)
dbutils.fs.mkdirs(sample_img_dir)
files =  dbutils.fs.ls(img_dir + "/daisy")[0:10] + dbutils.fs.ls(img_dir + "/tulips")[0:1]
for f in files:
  dbutils.fs.cp(f.path, sample_img_dir)

dbutils.fs.ls(sample_img_dir)


# COMMAND ----------
# 본문 내용은 여기서 부터 시작입니다.

# Spark 2.3 버전으로 실행해야 합니다.
# 스파크 딥러닝 관련 내용은 다음의 링크를 참조하십시오.
# https://docs.databricks.com/applications/deep-learning/deep-learning-pipelines.html

# spark-deep-learning 프로젝트는 다음의 링크를 참조하십시오.
# https://github.com/databricks/spark-deep-learning

from pyspark.ml.image import ImageSchema

# 이미지 파일이 많기 때문에 /tulips 디렉터리와 /daisy 디렉터리의 일부 파일을 /sample 디렉터리에 복제하여 사용합니다.
# 약 10개의 파일을 /sample 디렉터리에 복제합니다.
img_dir = '/data/deep-learning-images/'
sample_img_dir = img_dir + "/sample"

image_df = ImageSchema.readImages(sample_img_dir)

# COMMAND ----------

image_df.printSchema()


# COMMAND ----------

from pyspark.ml.image import ImageSchema
from pyspark.sql.functions import lit
from sparkdl.image import imageIO

tulips_df = ImageSchema.readImages(img_dir + "/tulips").withColumn("label", lit(1))
daisy_df = imageIO.readImagesWithCustomFn(img_dir + "/daisy", decode_f=imageIO.PIL_decode).withColumn("label", lit(0))
tulips_train, tulips_test = tulips_df.randomSplit([0.6, 0.4])
daisy_train, daisy_test = daisy_df.randomSplit([0.6, 0.4])
train_df = tulips_train.unionAll(daisy_train)
test_df = tulips_test.unionAll(daisy_test)

# 메모리 오버헤드를 줄이기 위해 파티션을 나눕니다.
train_df = train_df.repartition(100)
test_df = test_df.repartition(100)


# COMMAND ----------

from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from sparkdl import DeepImageFeaturizer

featurizer = DeepImageFeaturizer(inputCol="image", outputCol="features", modelName="InceptionV3")
lr = LogisticRegression(maxIter=20, regParam=0.05, elasticNetParam=0.3, labelCol="label")
p = Pipeline(stages=[featurizer, lr])

p_model = p.fit(train_df)


# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

tested_df = p_model.transform(test_df)
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
print("Test set accuracy = " + str(evaluator.evaluate(tested_df.select("prediction", "label"))))


# COMMAND ----------

from pyspark.sql.types import DoubleType
from pyspark.sql.functions import expr

def _p1(v):
  return float(v.array[1])

p1 = udf(_p1, DoubleType())
df = tested_df.withColumn("p_1", p1(tested_df.probability))
wrong_df = df.orderBy(expr("abs(p_1 - label)"), ascending=False)
wrong_df.select("image.origin", "p_1", "label").limit(10)


# COMMAND ----------

from pyspark.ml.image import ImageSchema
from sparkdl import DeepImagePredictor

# 빠른 테스트를 위해 앞서 선언한 샘플 이미지 디렉터리를 사용합니다.
image_df = ImageSchema.readImages(sample_img_dir)

predictor = DeepImagePredictor(inputCol="image", outputCol="predicted_labels", modelName="InceptionV3", decodePredictions=True, topK=10)
predictions_df = predictor.transform(image_df)


# COMMAND ----------

df = p_model.transform(image_df)
df.select("image.origin", (1-p1(df.probability)).alias("p_daisy")).show()


# COMMAND ----------

from keras.applications import InceptionV3
from sparkdl.udf.keras_image_model import registerKerasImageUDF
from keras.applications import InceptionV3

registerKerasImageUDF("my_keras_inception_udf", InceptionV3(weights="imagenet"))

# COMMAND ----------

