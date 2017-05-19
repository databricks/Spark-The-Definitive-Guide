package com.databricks.example

/**
  * Created by bill on 3/29/17.
  */

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

case class Person(Name: String, Company: String)

object DatasetUtils extends Serializable {
  @transient lazy val logger = Logger.getLogger(getClass.getName)

  def createPersonFromString(rawString: String): Person = {
    logger.info("people from string")
    val split = rawString.split(",")

    if (split.length < 2) {
      logger.warn("not enough fields in this string")
      Person(null, null)
    } else if (split.length == 2) {
      Person(split(0), split(1))
    } else {
      logger.warn("Too many fields in this string")
      Person(null, null)
    }
  }
}

object DatasetMainClass extends Serializable {
  def main(args: Array[String]) = {

    val spark = SparkSession
      .builder()
      .appName("Databricks Spark Example")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()
    // allows the .toDF method
    import spark.implicits._

    val authors = Seq("bill,databricks", "matei,databricks")
    val authorsDataset = spark
      .sparkContext
      .parallelize(authors)
      .map(DatasetUtils.createPersonFromString(_))
      .toDF()
      .as[Person]
      .show()

  }
}


