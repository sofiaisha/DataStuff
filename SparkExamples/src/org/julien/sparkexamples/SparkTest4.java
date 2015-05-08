package org.julien.sparkexamples;

import java.util.Map;
import java.util.function.Function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class SparkTest4 {

	/* 
	   Illustrate map(), count() and distinct()
	   
	   Build:
	   		mvn clean && mvn compile && mvn package
	  
	   Launch: 
	  		$SPARK_HOME/bin/spark-submit \
	  		--class org.julien.sparkexamples.SparkTest4 \
	  		target/SparkExamples-0.0.1-SNAPSHOT.jar \
	  		system.log result
	 */
	
	public static void main(String[] args) throws Exception {
		String inputFile = args[0];
		String outputFile = args[1];
		
		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setAppName("TimeOfDay");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Load input data.
		JavaRDD<String> inputRDD = sc.textFile(inputFile);

		// Split fields and keep only the 4th one (time of day)
		JavaRDD<String> timeRDD = inputRDD.map(x-> (x.split(" "))[3]);
						
		// Save results
		timeRDD.saveAsTextFile(outputFile);
		
		System.out.println("Line count: "+timeRDD.count());
		System.out.println("Unique timestamps: "+timeRDD.distinct().count());
		
		Map<String, Long> counters = timeRDD.countByValue();
		for (String s:counters.keySet()) {
			System.out.println(s+" : "+counters.get(s));
		}
	}
}
