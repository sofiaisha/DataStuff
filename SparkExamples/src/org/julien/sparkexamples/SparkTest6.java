package org.julien.sparkexamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.base.Optional;

import scala.Tuple2;



public class SparkTest6 {

	/* 
	   Illustrate JavaPairRDD and its methods for 2 RDDs
	   
	   Build:
	   		mvn clean && mvn compile && mvn package
	  
	   Launch: 
	  		$SPARK_HOME/bin/spark-submit \
	  		--class org.julien.sparkexamples.SparkTest6 \
	  		target/SparkExamples-0.0.1-SNAPSHOT.jar
	 */
	
	public static void main(String[] args) throws Exception {
		
		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setAppName("KeyValue");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Load input data.
		JavaRDD<String> inputRDD1 = sc.textFile("SparkTest5.txt");
		JavaRDD<String> inputRDD2 = sc.textFile("SparkTest6.txt");
		
		// Split fields to create key/value pair
		JavaPairRDD<String, String> pairRDD1 = 
				inputRDD1.mapToPair(
						x-> new Tuple2<String,String>(x.split(" ")[0], x.split(" ")[1])
				);
		
		JavaPairRDD<String, String> pairRDD2 = 
				inputRDD2.mapToPair(
						x-> new Tuple2<String,String>(x.split(" ")[0], x.split(" ")[1])
				);
				
		// pairRDD1 - pairRDD2
		JavaPairRDD<String, String> resultRDD1 = pairRDD1.subtractByKey(pairRDD2);
		resultRDD1.saveAsTextFile("SparkTest6_subtractByKey");
		
		// join 
		JavaPairRDD<String, Tuple2<String, String>> resultRDD2 = pairRDD1.join(pairRDD2);
		resultRDD2.saveAsTextFile("SparkTest6_join");
		
		// right outer join
		JavaPairRDD<String, Tuple2<Optional<String>, String>> resultRDD3 = pairRDD1.rightOuterJoin(pairRDD2);
		resultRDD3.saveAsTextFile("SparkTest6_rightjoin");
		
		// left outer join
		JavaPairRDD<String, Tuple2<String, Optional<String>>> resultRDD4 = pairRDD1.leftOuterJoin(pairRDD2);
		resultRDD4.saveAsTextFile("SparkTest6_leftjoin");
		
		// cogroup
		JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> resultRDD5 = pairRDD1.cogroup(pairRDD2);
		resultRDD5.saveAsTextFile("SparkTest6_cogroup");
		
	}
}
