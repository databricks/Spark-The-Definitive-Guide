package com.databricks.example

/**
  * Created by bill on 3/30/17.
  */


class DatasetExampleTest extends BaseSpec {
  import testImplicits._

  "Well formatted strings" should "just work" in {
    val authors = Seq("bill,databricks", "matei,databricks")
    val authorsRDD = spark
      .sparkContext
      .parallelize(authors)
      .map(DatasetUtils.createPersonFromString(_))

    val authorsDataset = spark.createDataFrame(authorsRDD).as[Person]

    assert(authorsDataset.count() == 2)
  }

  "Poorly formatted strings" should "not work" in {
    val authors = Seq(
      "matei",
      "bill",
      "bill,databricks,matei,databricks",
      "poorly formatted")

    val authorsDataset = spark
      .sparkContext
      .parallelize(authors)
      .map(DatasetUtils.createPersonFromString(_))
      .toDF()
      .as[Person]

    authorsDataset.show()
    assert(authorsDataset.where("name is not null").count() == 0)
  }

}
