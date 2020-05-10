// in Scala
spark.conf.set("spark.sql.shuffle.partitions", 5)
val static = spark.read.json("/data/activity-data")
val streaming = spark
  .readStream
  .schema(static.schema)
  .option("maxFilesPerTrigger", 10)
  .json("/data/activity-data")


// COMMAND ----------

streaming.printSchema()


// COMMAND ----------

// in Scala
val withEventTime = streaming.selectExpr(
  "*",
  "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time")


// COMMAND ----------

// in Scala
import org.apache.spark.sql.functions.{window, col}
withEventTime.groupBy(window(col("event_time"), "10 minutes")).count()
  .writeStream
  .queryName("events_per_window")
  .format("memory")
  .outputMode("complete")
  .start()


// COMMAND ----------

spark.sql("SELECT * FROM events_per_window").printSchema()


// COMMAND ----------

// in Scala
import org.apache.spark.sql.functions.{window, col}
withEventTime.groupBy(window(col("event_time"), "10 minutes"), "User").count()
  .writeStream
  .queryName("events_per_window")
  .format("memory")
  .outputMode("complete")
  .start()


// COMMAND ----------

// in Scala
import org.apache.spark.sql.functions.{window, col}
withEventTime.groupBy(window(col("event_time"), "10 minutes", "5 minutes"))
  .count()
  .writeStream
  .queryName("events_per_window")
  .format("memory")
  .outputMode("complete")
  .start()


// COMMAND ----------

// in Scala
import org.apache.spark.sql.functions.{window, col}
withEventTime
  .withWatermark("event_time", "5 hours")
  .groupBy(window(col("event_time"), "10 minutes", "5 minutes"))
  .count()
  .writeStream
  .queryName("events_per_window")
  .format("memory")
  .outputMode("complete")
  .start()


// COMMAND ----------

// in Scala
import org.apache.spark.sql.functions.expr

withEventTime
  .withWatermark("event_time", "5 seconds")
  .dropDuplicates("User", "event_time")
  .groupBy("User")
  .count()
  .writeStream
  .queryName("deduplicated")
  .format("memory")
  .outputMode("complete")
  .start()


// COMMAND ----------

case class InputRow(user:String, timestamp:java.sql.Timestamp, activity:String)
case class UserState(user:String,
  var activity:String,
  var start:java.sql.Timestamp,
  var end:java.sql.Timestamp)


// COMMAND ----------

def updateUserStateWithEvent(state:UserState, input:InputRow):UserState = {
  if (Option(input.timestamp).isEmpty) {
    return state
  }
  if (state.activity == input.activity) {

    if (input.timestamp.after(state.end)) {
      state.end = input.timestamp
    }
    if (input.timestamp.before(state.start)) {
      state.start = input.timestamp
    }
  } else {
    if (input.timestamp.after(state.end)) {
      state.start = input.timestamp
      state.end = input.timestamp
      state.activity = input.activity
    }
  }

  state
}


// COMMAND ----------

import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, GroupState}
def updateAcrossEvents(user:String,
  inputs: Iterator[InputRow],
  oldState: GroupState[UserState]):UserState = {
  var state:UserState = if (oldState.exists) oldState.get else UserState(user,
        "",
        new java.sql.Timestamp(6284160000000L),
        new java.sql.Timestamp(6284160L)
    )
  // we simply specify an old date that we can compare against and
  // immediately update based on the values in our data

  for (input <- inputs) {
    state = updateUserStateWithEvent(state, input)
    oldState.update(state)
  }
  state
}


// COMMAND ----------

import org.apache.spark.sql.streaming.GroupStateTimeout
withEventTime
  .selectExpr("User as user",
    "cast(Creation_Time/1000000000 as timestamp) as timestamp", "gt as activity")
  .as[InputRow]
  .groupByKey(_.user)
  .mapGroupsWithState(GroupStateTimeout.NoTimeout)(updateAcrossEvents)
  .writeStream
  .queryName("events_per_window")
  .format("memory")
  .outputMode("update")
  .start()


// COMMAND ----------

case class InputRow(device: String, timestamp: java.sql.Timestamp, x: Double)
case class DeviceState(device: String, var values: Array[Double],
  var count: Int)
case class OutputRow(device: String, previousAverage: Double)


// COMMAND ----------

def updateWithEvent(state:DeviceState, input:InputRow):DeviceState = {
  state.count += 1
  // maintain an array of the x-axis values
  state.values = state.values ++ Array(input.x)
  state
}


// COMMAND ----------

import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode,
  GroupState}

def updateAcrossEvents(device:String, inputs: Iterator[InputRow],
  oldState: GroupState[DeviceState]):Iterator[OutputRow] = {
  inputs.toSeq.sortBy(_.timestamp.getTime).toIterator.flatMap { input =>
    val state = if (oldState.exists) oldState.get
      else DeviceState(device, Array(), 0)

    val newState = updateWithEvent(state, input)
    if (newState.count >= 500) {
      // One of our windows is complete; replace our state with an empty
      // DeviceState and output the average for the past 500 items from
      // the old state
      oldState.update(DeviceState(device, Array(), 0))
      Iterator(OutputRow(device,
        newState.values.sum / newState.values.length.toDouble))
    }
    else {
      // Update the current DeviceState object in place and output no
      // records
      oldState.update(newState)
      Iterator()
    }
  }
}


// COMMAND ----------

import org.apache.spark.sql.streaming.GroupStateTimeout

withEventTime
  .selectExpr("Device as device",
    "cast(Creation_Time/1000000000 as timestamp) as timestamp", "x")
  .as[InputRow]
  .groupByKey(_.device)
  .flatMapGroupsWithState(OutputMode.Append,
    GroupStateTimeout.NoTimeout)(updateAcrossEvents)
  .writeStream
  .queryName("count_based_device")
  .format("memory")
  .outputMode("append")
  .start()


// COMMAND ----------

case class InputRow(uid:String, timestamp:java.sql.Timestamp, x:Double,
  activity:String)
case class UserSession(val uid:String, var timestamp:java.sql.Timestamp,
  var activities: Array[String], var values: Array[Double])
case class UserSessionOutput(val uid:String, var activities: Array[String],
  var xAvg:Double)


// COMMAND ----------

def updateWithEvent(state:UserSession, input:InputRow):UserSession = {
  // handle malformed dates
  if (Option(input.timestamp).isEmpty) {
    return state
  }

  state.timestamp = input.timestamp
  state.values = state.values ++ Array(input.x)
  if (!state.activities.contains(input.activity)) {
    state.activities = state.activities ++ Array(input.activity)
  }
  state
}


// COMMAND ----------

import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode,
  GroupState}

def updateAcrossEvents(uid:String,
  inputs: Iterator[InputRow],
  oldState: GroupState[UserSession]):Iterator[UserSessionOutput] = {

  inputs.toSeq.sortBy(_.timestamp.getTime).toIterator.flatMap { input =>
    val state = if (oldState.exists) oldState.get else UserSession(
    uid,
    new java.sql.Timestamp(6284160000000L),
    Array(),
    Array())
    val newState = updateWithEvent(state, input)

    if (oldState.hasTimedOut) {
      val state = oldState.get
      oldState.remove()
      Iterator(UserSessionOutput(uid,
      state.activities,
      newState.values.sum / newState.values.length.toDouble))
    } else if (state.values.length > 1000) {
      val state = oldState.get
      oldState.remove()
      Iterator(UserSessionOutput(uid,
      state.activities,
      newState.values.sum / newState.values.length.toDouble))
    } else {
      oldState.update(newState)
      oldState.setTimeoutTimestamp(newState.timestamp.getTime(), "5 seconds")
      Iterator()
    }

  }
}


// COMMAND ----------

import org.apache.spark.sql.streaming.GroupStateTimeout

withEventTime.where("x is not null")
  .selectExpr("user as uid",
    "cast(Creation_Time/1000000000 as timestamp) as timestamp",
    "x", "gt as activity")
  .as[InputRow]
  .withWatermark("timestamp", "5 seconds")
  .groupByKey(_.uid)
  .flatMapGroupsWithState(OutputMode.Append,
    GroupStateTimeout.EventTimeTimeout)(updateAcrossEvents)
  .writeStream
  .queryName("count_based_device")
  .format("memory")
  .start()


// COMMAND ----------

