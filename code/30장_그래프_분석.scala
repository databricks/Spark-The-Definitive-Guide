// 스칼라 버전
val bikeStations = spark.read.option("header","true")
  .csv("/data/bike-data/201508_station_data.csv")
val tripData = spark.read.option("header","true")
  .csv("/data/bike-data/201508_trip_data.csv")


// COMMAND ----------

// 스칼라 버전
val stationVertices = bikeStations.withColumnRenamed("name", "id").distinct()
val tripEdges = tripData
  .withColumnRenamed("Start Station", "src")
  .withColumnRenamed("End Station", "dst")


// COMMAND ----------

// 스칼라 버전

// graphframes(https://spark-packages.org/package/graphframes/graphframes) 라이브러리가 필요합니다.
// http://graphframes.github.io/quick-start.html
// DataBricks Runtime: https://docs.databricks.com/user-guide/libraries.html#maven-libraries

import org.graphframes.GraphFrame
val stationGraph = GraphFrame(stationVertices, tripEdges)
stationGraph.cache()


// COMMAND ----------

// 스칼라 버전
println(s"Total Number of Stations: ${stationGraph.vertices.count()}")
println(s"Total Number of Trips in Graph: ${stationGraph.edges.count()}")
println(s"Total Number of Trips in Original Data: ${tripData.count()}")


// COMMAND ----------

// 스칼라 버전
import org.apache.spark.sql.functions.desc
stationGraph.edges.groupBy("src", "dst").count().orderBy(desc("count")).show(10)


// COMMAND ----------

// 스칼라 버전
stationGraph.edges
  .where("src = 'Townsend at 7th' OR dst = 'Townsend at 7th'")
  .groupBy("src", "dst").count()
  .orderBy(desc("count"))
  .show(10)


// COMMAND ----------

// 스칼라 버전
val townAnd7thEdges = stationGraph.edges
  .where("src = 'Townsend at 7th' OR dst = 'Townsend at 7th'")
val subgraph = GraphFrame(stationGraph.vertices, townAnd7thEdges)


// COMMAND ----------

// 스칼라 버전
val motifs = stationGraph.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[ca]->(a)")


// COMMAND ----------

// 스칼라 버전
import org.apache.spark.sql.functions.expr
motifs.selectExpr("*",
    "to_timestamp(ab.`Start Date`, 'MM/dd/yyyy HH:mm') as abStart",
    "to_timestamp(bc.`Start Date`, 'MM/dd/yyyy HH:mm') as bcStart",
    "to_timestamp(ca.`Start Date`, 'MM/dd/yyyy HH:mm') as caStart")
  .where("ca.`Bike #` = bc.`Bike #`").where("ab.`Bike #` = bc.`Bike #`")
  .where("a.id != b.id").where("b.id != c.id")
  .where("abStart < bcStart").where("bcStart < caStart")
  .orderBy(expr("cast(caStart as long) - cast(abStart as long)"))
  .selectExpr("a.id", "b.id", "c.id", "ab.`Start Date`", "ca.`End Date`")
  .limit(1).show(false)


// COMMAND ----------

// 스칼라 버전
import org.apache.spark.sql.functions.desc
val ranks = stationGraph.pageRank.resetProbability(0.15).maxIter(10).run()
ranks.vertices.orderBy(desc("pagerank")).select("id", "pagerank").show(10)


// COMMAND ----------

// 스칼라 버전
val inDeg = stationGraph.inDegrees
inDeg.orderBy(desc("inDegree")).show(5, false)


// COMMAND ----------

// 스칼라 버전
val outDeg = stationGraph.outDegrees
outDeg.orderBy(desc("outDegree")).show(5, false)


// COMMAND ----------

// 스칼라 버전
val degreeRatio = inDeg.join(outDeg, Seq("id"))
  .selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio")
degreeRatio.orderBy(desc("degreeRatio")).show(10, false)
degreeRatio.orderBy("degreeRatio").show(10, false)


// COMMAND ----------

// 스칼라 버전
stationGraph.bfs.fromExpr("id = 'Townsend at 7th'")
  .toExpr("id = 'Spear at Folsom'").maxPathLength(2).run().show(10)


// COMMAND ----------

// 스칼라 버전
spark.sparkContext.setCheckpointDir("/tmp/checkpoints")


// COMMAND ----------

// 스칼라 버전
val minGraph = GraphFrame(stationVertices, tripEdges.sample(false, 0.1))
val cc = minGraph.connectedComponents.run()


// COMMAND ----------

// 스칼라 버전
cc.where("component != 0").show()


// COMMAND ----------

// 스칼라 버전
val scc = minGraph.stronglyConnectedComponents.maxIter(3).run()


// COMMAND ----------

scc.groupBy("component").count().show()


// COMMAND ----------

