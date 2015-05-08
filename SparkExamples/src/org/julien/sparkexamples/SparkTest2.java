package org.julien.sparkexamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkTest2 {

	/* 
	   Illustrate filter() and union()
	   
	   Build:
	   		mvn clean && mvn compile && mvn package
	  
	   Launch: 
	  		$SPARK_HOME/bin/spark-submit \
	  		--class org.julien.sparkexamples.SparkTest2 \
	  		target/SparkExamples-0.0.1-SNAPSHOT.jar \
	  		system.log result
	 */
	
	public static void main(String[] args) throws Exception {
		String inputFile = args[0];
		String outputFile = args[1];
		
		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setAppName("FindErrors");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Load input data.
		JavaRDD<String> inputRDD = sc.textFile(inputFile);

		// Filter lines
		JavaRDD<String> timeoutRDD = inputRDD.filter(x -> {
			return (x.contains("timeout"));
		});
		
		// Filter lines in a different way
		JavaRDD<String> failureRDD = inputRDD.filter(x -> {
			return (x.contains("failure"));
		});

		// Join both results
		JavaRDD<String> badLines = failureRDD.union(timeoutRDD);
		
		// Save results
		badLines.saveAsTextFile(outputFile);
	}
}
