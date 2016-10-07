package com.oreilly.learningsparkexamples.mini.java;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import net.texhad3op.sparktest.dto.CallInfo;
import scala.Tuple2;

public class CallsCounter implements Serializable {

	public static void main(String[] args) {
		new CallsCounter().start();
	}

	public void start() {
		SparkConf conf = new SparkConf().setAppName("callsCounter");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> input0 = sc.textFile("/home/jurij/calls");

		// JavaRDD<CallInfo> calls = input0.filter(line ->
		// !line.trim().startsWith("#"))
		// .map(line -> produceCallInfo(line));

		JavaRDD<String> input = input0.filter(line -> !line.trim().startsWith("#"));
		// JavaPairRDD<String, CallInfo> counts = input.mapToPair(new
		// PairFunction<String, String, CallInfo>() {
		// public Tuple2<String, CallInfo> call(String x) {
		// CallInfo ci = produceCallInfo(x);
		// return new Tuple2(ci.getCaller(), ci);
		// }
		// });

		JavaPairRDD<String, Integer> counts = input.mapToPair(line -> {
			List<String> elements = Arrays.asList(line.split("\\s+"));
			return new Tuple2(elements.get(0), 1);
		});

		JavaPairRDD<String, Integer> res  = counts.reduceByKey((k1, k2) -> k1 + k2);
		res.saveAsTextFile("/home/jurij/res");
		// new PairFunction<String, String, CallInfo>() {
		// public Tuple2<String, CallInfo> call(String x) {
		// CallInfo ci = produceCallInfo(x);
		// return new Tuple2(ci.getCaller(), ci);
		// }
		// });

		List<Tuple2<String, Integer>> ll = res.collect();
		 System.out.println("===============================");
		 System.out.println("===============================");
		 System.out.println("===============================");
		 System.out.println(ll);
		 System.out.println("===============================");
		 System.out.println("===============================");
		 System.out.println("===============================");
		sc.close();
	}

	private CallInfo produceCallInfo(String line) {
		List<String> elements = Arrays.asList(line.split("\\s+"));
		// System.out.println("===================================");
		// System.out.println(elements);
		System.out.println("->" + elements.get(0));
		return new CallInfo(elements.get(0), elements.get(1), elements.get(2), elements.get(3));
	}
}
