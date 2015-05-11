package org.julien.sparkexamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;



public class SparkTest5 {

	/* 
	   Illustrate JavaPairRDD and its main methods
	   
	   Build:
	   		mvn clean && mvn compile && mvn package
	  
	   Launch: 
	  		$SPARK_HOME/bin/spark-submit \
	  		--class org.julien.sparkexamples.SparkTest5 \
	  		target/SparkExamples-0.0.1-SNAPSHOT.jar
	 */
	
	public static void main(String[] args) throws Exception {
		
		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setAppName("KeyValue");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Load input data.
		JavaRDD<String> inputRDD = sc.textFile("SparkTest5.txt");

		/* Version with no lambda : eeeek!
		PairFunction<String, String, String> keyFunc = 
				new PairFunction<String, String, String>() { 
					public Tuple2<String, String> call(String x) {
						return new Tuple2<String, String>(x.split(" ")[0], x.split(" ")[1]); 
					}
				};
		JavaPairRDD<String, String> pairs = inputRDD.mapToPair(keyFunc);
		*/
		
		// Split fields to create key/value pair
		JavaPairRDD<String, String> pairRDD = 
				inputRDD.mapToPair(
						x-> new Tuple2<String,String>(x.split(" ")[0], x.split(" ")[1])
				);
				
		// Save keys
		JavaRDD<String> resultRDD1 = pairRDD.keys();
		resultRDD1.saveAsTextFile("SparkTest5_keys");
		
		// Save values
		JavaRDD<String> resultRDD2 = pairRDD.values();
		resultRDD2.saveAsTextFile("SparkTest5_values");
		
		// Sort by key
		JavaPairRDD<String, String> resultRDD3 = pairRDD.sortByKey();
		resultRDD3.saveAsTextFile("SparkTest5_sortByKey");
		
		// Reduce by key
		JavaPairRDD<String, String> resultRDD4 = pairRDD.reduceByKey((x,y)->x+y);
		resultRDD4.saveAsTextFile("SparkTest5_reduceByKey");
		
		// Group by key
		JavaPairRDD<String, Iterable<String>> resultRDD5 = pairRDD.groupByKey();
		resultRDD5.saveAsTextFile("SparkTest5_groupByKey");
		
		// mapValues
		JavaPairRDD<String, String> resultRDD6 = pairRDD.mapValues(x->x.toUpperCase());
		resultRDD6.saveAsTextFile("SparkTest5_mapValues");
	}
}
