my_collection = "Spark The Definitive Guide : Big Data Processing Made Simple"\
  .split(" ")
words = spark.sparkContext.parallelize(my_collection, 2)


# COMMAND ----------

supplementalData = {"Spark":1000, "Definitive":200,
                    "Big":-300, "Simple":100}


# COMMAND ----------

suppBroadcast = spark.sparkContext.broadcast(supplementalData)


# COMMAND ----------

suppBroadcast.value


# COMMAND ----------

words.map(lambda word: (word, suppBroadcast.value.get(word, 0)))\
  .sortBy(lambda wordPair: wordPair[1])\
  .collect()


# COMMAND ----------

flights = spark.read\
  .parquet("/data/flight-data/parquet/2010-summary.parquet")


# COMMAND ----------

accChina = spark.sparkContext.accumulator(0)


# COMMAND ----------

def accChinaFunc(flight_row):
  destination = flight_row["DEST_COUNTRY_NAME"]
  origin = flight_row["ORIGIN_COUNTRY_NAME"]
  if destination == "China":
    accChina.add(flight_row["count"])
  if origin == "China":
    accChina.add(flight_row["count"])


# COMMAND ----------

flights.foreach(lambda flight_row: accChinaFunc(flight_row))


# COMMAND ----------

accChina.value # 953


# COMMAND ----------

