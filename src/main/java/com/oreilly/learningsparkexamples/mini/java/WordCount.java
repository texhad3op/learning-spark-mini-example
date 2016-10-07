package com.oreilly.learningsparkexamples.mini.java;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) {
		// // Create a Java Spark Context
		// SparkConf conf = new SparkConf().setAppName("wordCount");
		// JavaSparkContext sc = new JavaSparkContext(conf);
		// // Load our input data.
		// JavaRDD<String> input = sc.textFile("/home/jurij/text");
		// // Split up into words.
		// JavaRDD<String> words = input.flatMap(new FlatMapFunction<String,
		// String>() {
		// public Iterable<String> call(String x) {
		// return Arrays.asList(x.split(" "));
		// }
		// });
		// // Transform into pairs and count.
		// JavaPairRDD<String, Integer> counts = words.mapToPair(new
		// PairFunction<String, String, Integer>() {
		// public Tuple2<String, Integer> call(String x) {
		// return new Tuple2(x, 1);
		// }
		// }).reduceByKey(new Function2<Integer, Integer, Integer>() {
		// public Integer call(Integer x, Integer y) {
		// return x + y;
		// }
		// });
		// // Save the word count back out to a text file, causing evaluation.
		// counts.saveAsTextFile("/home/jurij/res");
		// sc.close();

		SparkConf conf = new SparkConf().setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> input0 = sc.textFile("/home/jurij/text").repartition(10).persist(StorageLevel.DISK_ONLY());


		//sc.sequenceFile(fileName, Text.class, IntWritable.class);
		//input0.persist(StorageLevel.DISK_ONLY());
		System.out.println("===============================================");
		System.out.println("===============================================");
		System.out.println("===============================================");
		input0.take(10).forEach(System.out::println);
		System.out.println("===============================================");
		System.out.println("===============================================");
		System.out.println("===============================================");
		JavaRDD<String> input = input0.filter(line -> !line.startsWith("#"));

		JavaRDD<String> words = input.flatMap(line -> Arrays.asList(line.split(" ")));

		JavaPairRDD<String, Integer> counts = words.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
				.reduceByKey((x, y) -> x + y);
		counts.saveAsTextFile("/home/jurij/res");
		sc.close();


	}

}