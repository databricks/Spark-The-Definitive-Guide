package com.databricks.example;

import org.apache.spark.sql.SparkSession;

/**
 * Created by bill on 4/4/17.
 */
public class SimpleExample {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .getOrCreate();

        System.out.println(spark.range(1, 2000).count());

    }
}
