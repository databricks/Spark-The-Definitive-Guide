package com.databricks.example

/**
  * Created by bill on 5/8/17.
  */
class DataFrameExampleTest extends BaseSpec {

  import testImplicits._

  "The UDF" should "work as expected" in {
    assert(DFUtils.pointlessUDF("something") == "something")
  }

  "Well formatted strings" should "just work" in {
    spark.udf.register("pointlessUDF", DFUtils.pointlessUDF(_: String): String)
    val authors = Seq("bill,databricks", "matei,databricks")
    val authorsDF = spark
      .sparkContext
      .parallelize(authors)
      .toDF("raw")
      .selectExpr("split(raw, ',') as values")
      .selectExpr("pointlessUDF(values[0]) as name", "values[1] as company")

    assert(authorsDF.count() == 2)
  }

  "Poorly formatted strings" should "not work" in {
    spark.udf.register("pointlessUDF", DFUtils.pointlessUDF(_: String): String)
    val authors = Seq(
      "matei",
      "bill",
      "bill,databricks,matei,databricks",
      "poorly formatted")

    val authorsDF = spark
      .sparkContext
      .parallelize(authors)
      .toDF("raw")
      .selectExpr("split(raw, ',') as values")
      .where("size(values) == 2")
      .selectExpr("pointlessUDF(values[0]) as name", "values[1] as company")

    authorsDF.show()
    assert(authorsDF.where("name is not null").count() == 0)
  }
}
