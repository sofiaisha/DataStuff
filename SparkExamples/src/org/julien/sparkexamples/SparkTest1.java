package org.julien.sparkexamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkTest1 {

	/* 
	   Illustrate filter() with a lamdba
	   
	   Build:
	   		mvn clean && mvn compile && mvn package
	  
	   Launch: 
	  		$SPARK_HOME/bin/spark-submit \
	  		--class org.julien.sparkexamples.SparkTest1 \
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

		// Save results
		timeoutRDD.saveAsTextFile(outputFile);
	}
}
