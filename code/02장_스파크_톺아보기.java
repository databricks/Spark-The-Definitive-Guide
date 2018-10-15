// This Java code was contributed by @neeleshkumar-mannur

import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.desc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class A_Gentle_Introduction_to_Spark_Chapter_2_A_Gentle_Introduction_to_Spark {
	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.master("local[*]")
				.appName("Chapter2AGentleIntorductionToSpark")
				.getOrCreate();
		
		// Creating a dataset 
		Dataset<Row> myRange = spark.range(1000).toDF("number");
		myRange.show();
		
		// Find even numbers in the dataset
		Dataset<Row> divisBy2 = myRange.where("number % 2 = 0");
		divisBy2.show();
		
		// Get the count of even number in the dataset
		System.out.println(divisBy2.count());
		
		// Loading the Flight Data 
		Dataset<Row> flightData2015 = spark
				.read()
				.option("inferSchema", "true")
				.option("header", "true")
				.csv("data/flight-data/csv/2015-summary.csv");
		
		// Taking 3 rows from the flight dataset
		Object [] dataObjects = (Object[]) flightData2015.take(3);
		for(Object object: dataObjects) {
			System.out.println(object);
		}
		
		// Explain the physical plan 
		flightData2015.sort("count").explain();
		
		// Repartitioning Data
		spark.conf().set("spark.sql.shuffle.partitions", "5");
		
		// Taking the repartitioned dataset
		Object [] repartitionedRows = (Object[]) flightData2015.sort("count").take(2);
		
		for(Object repartitionedRow: repartitionedRows) {
			System.out.println(repartitionedRow);
		}	
		
		// Creating a Temporary Table
		flightData2015.createOrReplaceTempView("flight_data_2015");
			
		// Firing SQL Query on the temporary table
		Dataset<Row> sqlWay = spark.sql("SELECT DEST_COUNTRY_NAME, count(1)\r\n" + 
				"FROM flight_data_2015\r\n" + 
				"GROUP BY DEST_COUNTRY_NAME");
		
		// Going by the Dataset way
		Dataset<Row> datasetWay = flightData2015.groupBy("DEST_COUNTRY_NAME").count();
		
		// Explanation for each of the ways
		sqlWay.explain();
		datasetWay.explain();
		
		// Taking 1 value out of the temporary table using SQL
		Object [] dataRows = (Object[])spark.sql("SELECT max(count) from flight_data_2015").take(1);
		
		for(Object dataRow: dataRows) {
			System.out.println(dataRow);
		}
		
		// Applying the max function
		Object [] maxRows = (Object[])flightData2015.select(max("count")).take(1);
		
		for(Object maxRow: maxRows) {
			System.out.println(maxRow);
		}
		
		Dataset<Row> maxSql = spark.sql("SELECT DEST_COUNTRY_NAME, sum(count) as destination_total\r\n" + 
				"FROM flight_data_2015\r\n" + 
				"GROUP BY DEST_COUNTRY_NAME\r\n" + 
				"ORDER BY sum(count) DESC\r\n" + 
				"LIMIT 5");
		
		maxSql.show();
		
		// Applying DESC function
		flightData2015
		  .groupBy("DEST_COUNTRY_NAME")
		  .sum("count")
		  .withColumnRenamed("sum(count)", "destination_total")
		  .sort(desc("destination_total"))
		  .limit(5)
		  .show();
		
		// Explain execution plan
		flightData2015
		  .groupBy("DEST_COUNTRY_NAME")
		  .sum("count")
		  .withColumnRenamed("sum(count)", "destination_total")
		  .sort(desc("destination_total"))
		  .limit(5)
		  .explain();
	}
}
