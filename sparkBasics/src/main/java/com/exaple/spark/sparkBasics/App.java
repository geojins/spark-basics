package com.exaple.spark.sparkBasics;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName(
				"spark-basics");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile("/home/jsg/Code/spark/spark-basics/sparkBasics/src/main/java/com/exaple/spark/sparkBasics/spark-word-count.txt");

		JavaRDD<String> words = lines
				.flatMap(new FlatMapFunction<String, String>() {

					public Iterable<String> call(String arg0) throws Exception {
						return Arrays.asList(arg0.split(" "));
					}
				});

		JavaPairRDD<String, Integer> wordCount = words
				.mapToPair(new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String arg0) {
						return new Tuple2<String, Integer>(arg0, 1);
					}
				});

		JavaPairRDD<String, Integer> wordCountAggregate = wordCount
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					public Integer call(Integer v1, Integer v2)
							throws Exception {

						return v1 + v2;
					}
				});

		//wordCountAggregate.saveAsTextFile("/home/jsg/Code/spark/spark-basics/sparkBasics/src/main/java/com/exaple/spark/sparkBasics/spark-word-count-output.txt");
		
		for(Tuple2<String,Integer> tuple :wordCountAggregate.sortByKey().collect() ) {
			System.out.println(tuple);
		}

	}
}
