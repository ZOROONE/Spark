package com.spark.sparkstreaming;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;

import scala.Tuple2;

public class Driver_HA {
	
	public static void main(String[] args) {
		
		final SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("masterHA");
		final String checkpointDir = "./spark/checkpoint";
		
		JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {
			@Override
			public JavaStreamingContext create() {
				return getOrCreate(checkpointDir,conf);
			}

		};
		
		JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(checkpointDir, factory);
		
		JavaSparkContext sc = jsc.sparkContext();
		sc.setLogLevel("WARN");
		jsc.start();
		jsc.awaitTermination();
		jsc.stop();
	}

	public static JavaStreamingContext getOrCreate(String checkpointDir, SparkConf conf) {
		SparkConf SparkConf = conf;
		
		System.out.println("create JavaStreamingContext----------");
		JavaStreamingContext jsc = new JavaStreamingContext(SparkConf, Durations.seconds(5));
		
		jsc.checkpoint(checkpointDir);
		JavaDStream<String> lineDStream = jsc.textFileStream("./spark/data");
		
		JavaPairDStream<String, Integer> wordDStream = lineDStream.flatMapToPair(
				new PairFlatMapFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, Integer>> call(String t) throws Exception {
				List<Tuple2<String, Integer>> list = new ArrayList<>();
				for(String word: t.split(" ")) {
					list.add(new Tuple2<String, Integer>(word, 1));
				}
				return list;
			}
		});
		
		JavaPairDStream<String, Integer> result = wordDStream.reduceByKey(
				new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				System.out.println("------reduceByKey");
				return v1 + v2;
			}
		});
		
		result.print();
		return jsc;
	}
	
	
}
