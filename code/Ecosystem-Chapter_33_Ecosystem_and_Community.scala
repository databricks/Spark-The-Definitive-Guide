// allows us to include spark packages
resolvers += "bintray-spark-packages" at
  "https://dl.bintray.com/spark-packages/maven/"


// COMMAND ----------

libraryDependencies ++= Seq(
...
  // spark packages
  "graphframes" % "graphframes" % "0.4.0-spark2.1-s_2.11",

)


// COMMAND ----------

