// 스칼라 버전
val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
  .split(" ")
val words = spark.sparkContext.parallelize(myCollection, 2)


// COMMAND ----------

// 스칼라 버전
words.map(word => (word.toLowerCase, 1))


// COMMAND ----------

// 스칼라 버전
val keyword = words.keyBy(word => word.toLowerCase.toSeq(0).toString)


// COMMAND ----------

// 스칼라 버전
keyword.mapValues(word => word.toUpperCase).collect()


// COMMAND ----------

// 스칼라 버전
keyword.flatMapValues(word => word.toUpperCase).collect()


// COMMAND ----------

// 스칼라 버전
keyword.keys.collect()
keyword.values.collect()


// COMMAND ----------

keyword.lookup("s")


// COMMAND ----------

// 스칼라 버전
val distinctChars = words.flatMap(word => word.toLowerCase.toSeq).distinct
  .collect()
import scala.util.Random
val sampleMap = distinctChars.map(c => (c, new Random().nextDouble())).toMap
words.map(word => (word.toLowerCase.toSeq(0), word))
  .sampleByKey(true, sampleMap, 6L)
  .collect()


// COMMAND ----------

// 스칼라 버전
words.map(word => (word.toLowerCase.toSeq(0), word))
  .sampleByKeyExact(true, sampleMap, 6L).collect()


// COMMAND ----------

// 스칼라 버전
val chars = words.flatMap(word => word.toLowerCase.toSeq)
val KVcharacters = chars.map(letter => (letter, 1))
def maxFunc(left:Int, right:Int) = math.max(left, right)
def addFunc(left:Int, right:Int) = left + right
val nums = sc.parallelize(1 to 30, 5)


// COMMAND ----------

// 스칼라 버전
val timeout = 1000L //milliseconds
val confidence = 0.95
KVcharacters.countByKey()
KVcharacters.countByKeyApprox(timeout, confidence)


// COMMAND ----------

// 스칼라 버전
KVcharacters.groupByKey().map(row => (row._1, row._2.reduce(addFunc))).collect()


// COMMAND ----------

KVcharacters.reduceByKey(addFunc).collect()


// COMMAND ----------

// 스칼라 버전
nums.aggregate(0)(maxFunc, addFunc)


// COMMAND ----------

// 스칼라 버전
val depth = 3
nums.treeAggregate(0)(maxFunc, addFunc, depth)


// COMMAND ----------

// 스칼라 버전
KVcharacters.aggregateByKey(0)(addFunc, maxFunc).collect()


// COMMAND ----------

// 스칼라 버전
val valToCombiner = (value:Int) => List(value)
val mergeValuesFunc = (vals:List[Int], valToAppend:Int) => valToAppend :: vals
val mergeCombinerFunc = (vals1:List[Int], vals2:List[Int]) => vals1 ::: vals2
// now we define these as function variables
val outputPartitions = 6
KVcharacters
  .combineByKey(
    valToCombiner,
    mergeValuesFunc,
    mergeCombinerFunc,
    outputPartitions)
  .collect()


// COMMAND ----------

// 스칼라 버전
KVcharacters.foldByKey(0)(addFunc).collect()


// COMMAND ----------

// 스칼라 버전
import scala.util.Random
val distinctChars = words.flatMap(word => word.toLowerCase.toSeq).distinct
val charRDD = distinctChars.map(c => (c, new Random().nextDouble()))
val charRDD2 = distinctChars.map(c => (c, new Random().nextDouble()))
val charRDD3 = distinctChars.map(c => (c, new Random().nextDouble()))
charRDD.cogroup(charRDD2, charRDD3).take(5)


// COMMAND ----------

// 스칼라 버전
val keyedChars = distinctChars.map(c => (c, new Random().nextDouble()))
val outputPartitions = 10
KVcharacters.join(keyedChars).count()
KVcharacters.join(keyedChars, outputPartitions).count()


// COMMAND ----------

// 스칼라 버전
val numRange = sc.parallelize(0 to 9, 2)
words.zip(numRange).collect()


// COMMAND ----------

// 스칼라 버전
words.coalesce(1).getNumPartitions // 1


// COMMAND ----------

words.repartition(10) // gives us 10 partitions


// COMMAND ----------

// 스칼라 버전
val df = spark.read.option("header", "true").option("inferSchema", "true")
  .csv("/data/retail-data/all/")
val rdd = df.coalesce(10).rdd


// COMMAND ----------

df.printSchema()


// COMMAND ----------

// 스칼라 버전
import org.apache.spark.HashPartitioner
rdd.map(r => r(6)).take(5).foreach(println)
val keyedRDD = rdd.keyBy(row => row(6).asInstanceOf[Int].toDouble)


// COMMAND ----------

keyedRDD.partitionBy(new HashPartitioner(10)).take(10)


// COMMAND ----------

// 스칼라 버전
import org.apache.spark.Partitioner
class DomainPartitioner extends Partitioner {
 def numPartitions = 3
 def getPartition(key: Any): Int = {
   val customerId = key.asInstanceOf[Double].toInt
   if (customerId == 17850.0 || customerId == 12583.0) {
     return 0
   } else {
     return new java.util.Random().nextInt(2) + 1
   }
 }
}

keyedRDD
  .partitionBy(new DomainPartitioner).map(_._1).glom().map(_.toSet.toSeq.length)
  .take(5)


// COMMAND ----------

// 스칼라 버전
class SomeClass extends Serializable {
  var someValue = 0
  def setSomeValue(i:Int) = {
    someValue = i
    this
  }
}

sc.parallelize(1 to 10).map(num => new SomeClass().setSomeValue(num))


// COMMAND ----------

// 스칼라 버전
val conf = new SparkConf().setMaster(...).setAppName(...)
conf.registerKryoClasses(Array(classOf[MyClass1], classOf[MyClass2]))
val sc = new SparkContext(conf)


// COMMAND ----------

