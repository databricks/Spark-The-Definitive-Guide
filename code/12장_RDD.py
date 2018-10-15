spark.range(10).rdd


# COMMAND ----------

spark.range(10).toDF("id").rdd.map(lambda row: row[0])


# COMMAND ----------

spark.range(10).rdd.toDF()


# COMMAND ----------

myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"\
  .split(" ")
words = spark.sparkContext.parallelize(myCollection, 2)


# COMMAND ----------

words.setName("myWords")
words.name() # myWords


# COMMAND ----------

def startsWithS(individual):
  return individual.startswith("S")


# COMMAND ----------

words.filter(lambda word: startsWithS(word)).collect()


# COMMAND ----------

words2 = words.map(lambda word: (word, word[0], word.startswith("S")))


# COMMAND ----------

words2.filter(lambda record: record[2]).take(5)


# COMMAND ----------

words.flatMap(lambda word: list(word)).take(5)


# COMMAND ----------

words.sortBy(lambda word: len(word) * -1).take(2)


# COMMAND ----------

fiftyFiftySplit = words.randomSplit([0.5, 0.5])


# COMMAND ----------

spark.sparkContext.parallelize(range(1, 21)).reduce(lambda x, y: x + y) # 210


# COMMAND ----------

def wordLengthReducer(leftWord, rightWord):
  if len(leftWord) > len(rightWord):
    return leftWord
  else:
    return rightWord

words.reduce(wordLengthReducer)


# COMMAND ----------

words.getStorageLevel()


# COMMAND ----------

words.mapPartitions(lambda part: [1]).sum() # 2


# COMMAND ----------

def indexedFunc(partitionIndex, withinPartIterator):
  return ["partition: {} => {}".format(partitionIndex,
    x) for x in withinPartIterator]
words.mapPartitionsWithIndex(indexedFunc).collect()


# COMMAND ----------

spark.sparkContext.parallelize(["Hello", "World"], 2).glom().collect()
# [['Hello'], ['World']]


# COMMAND ----------

