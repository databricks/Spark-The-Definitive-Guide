This Java code was contributed by @neeleshkumar-mannur

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Structured_APIs_Chapter_4_Structured_API_Overview {
	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.master("local[*]")
				.appName("Chapter4StructuredAPIOverview")
				.getOrCreate();
		
		Dataset<Row> df = spark.range(500).toDF("number");
		df.select(df.col("number").plus(10)).show();
		
		Object [] dataObjects = (Object[])spark.range(2).toDF().collect();
		for(Object object: dataObjects) {
			System.out.println(object.toString());
		}
		
	}
}
