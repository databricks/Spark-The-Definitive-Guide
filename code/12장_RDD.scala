spark.sparkContext


// COMMAND ----------

// 스칼라 버전: converts a Dataset[Long] to RDD[Long]
spark.range(500).rdd


// COMMAND ----------

// 스칼라 버전
spark.range(10).toDF().rdd.map(rowObject => rowObject.getLong(0))


// COMMAND ----------

// 스칼라 버전
spark.range(10).rdd.toDF()


// COMMAND ----------

// 스칼라 버전
val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
  .split(" ")
val words = spark.sparkContext.parallelize(myCollection, 2)


// COMMAND ----------

// 스칼라 버전
words.setName("myWords")
words.name // myWords


// COMMAND ----------

spark.sparkContext.textFile("/some/path/withTextFiles")


// COMMAND ----------

spark.sparkContext.wholeTextFiles("/some/path/withTextFiles")


// COMMAND ----------

words.distinct().count()


// COMMAND ----------

// 스칼라 버전
def startsWithS(individual:String) = {
  individual.startsWith("S")
}


// COMMAND ----------

// 스칼라 버전
words.filter(word => startsWithS(word)).collect()


// COMMAND ----------

// 스칼라 버전
val words2 = words.map(word => (word, word(0), word.startsWith("S")))


// COMMAND ----------

// 스칼라 버전
words2.filter(record => record._3).take(5)


// COMMAND ----------

// 스칼라 버전
words.flatMap(word => word.toSeq).take(5)


// COMMAND ----------

// 스칼라 버전
words.sortBy(word => word.length() * -1).take(2)


// COMMAND ----------

// 스칼라 버전
val fiftyFiftySplit = words.randomSplit(Array[Double](0.5, 0.5))


// COMMAND ----------

// 스칼라 버전
spark.sparkContext.parallelize(1 to 20).reduce(_ + _) // 210


// COMMAND ----------

// 스칼라 버전
def wordLengthReducer(leftWord:String, rightWord:String): String = {
  if (leftWord.length > rightWord.length)
    return leftWord
  else
    return rightWord
}

words.reduce(wordLengthReducer)


// COMMAND ----------

words.count()


// COMMAND ----------

val confidence = 0.95
val timeoutMilliseconds = 400
words.countApprox(timeoutMilliseconds, confidence)


// COMMAND ----------

words.countApproxDistinct(0.05)


// COMMAND ----------

words.countApproxDistinct(4, 10)


// COMMAND ----------

words.countByValue()


// COMMAND ----------

words.countByValueApprox(1000, 0.95)


// COMMAND ----------

words.first()


// COMMAND ----------

spark.sparkContext.parallelize(1 to 20).max()
spark.sparkContext.parallelize(1 to 20).min()


// COMMAND ----------

words.take(5)
words.takeOrdered(5)
words.top(5)
val withReplacement = true
val numberToTake = 6
val randomSeed = 100L
words.takeSample(withReplacement, numberToTake, randomSeed)


// COMMAND ----------

words.saveAsTextFile("file:/tmp/bookTitle")


// COMMAND ----------

// 스칼라 버전
import org.apache.hadoop.io.compress.BZip2Codec
words.saveAsTextFile("file:/tmp/bookTitleCompressed", classOf[BZip2Codec])


// COMMAND ----------

words.saveAsObjectFile("/tmp/my/sequenceFilePath")


// COMMAND ----------

words.cache()


// COMMAND ----------

// 스칼라 버전
words.getStorageLevel


// COMMAND ----------

spark.sparkContext.setCheckpointDir("/some/path/for/checkpointing")
words.checkpoint()


// COMMAND ----------

words.pipe("wc -l").collect()


// COMMAND ----------

// 스칼라 버전
words.mapPartitions(part => Iterator[Int](1)).sum() // 2


// COMMAND ----------

// 스칼라 버전
def indexedFunc(partitionIndex:Int, withinPartIterator: Iterator[String]) = {
  withinPartIterator.toList.map(
    value => s"Partition: $partitionIndex => $value").iterator
}
words.mapPartitionsWithIndex(indexedFunc).collect()


// COMMAND ----------

words.foreachPartition { iter =>
  import java.io._
  import scala.util.Random
  val randomFileName = new Random().nextInt()
  val pw = new PrintWriter(new File(s"/tmp/random-file-${randomFileName}.txt"))
  while (iter.hasNext) {
      pw.write(iter.next())
  }
  pw.close()
}


// COMMAND ----------

// 스칼라 버전
spark.sparkContext.parallelize(Seq("Hello", "World"), 2).glom().collect()
// Array(Array(Hello), Array(World))


// COMMAND ----------

