// 스칼라 버전
import org.apache.spark.SparkConf
val conf = new SparkConf().setMaster("local[2]").setAppName("DefinitiveGuide")
  .set("some.conf", "to.some.value")


// COMMAND ----------

