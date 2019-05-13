package com.spark.sparkstreaming;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class FromSocket {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf();
		conf.setMaster("local[2]").setAppName("fromsocket");
		
		JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.seconds(5));
		
		JavaReceiverInputDStream<String> lineDStream = javaStreamingContext.socketTextStream("node001", 9999);
		
		JavaPairDStream<String, Integer> flatMapToPair = lineDStream.flatMapToPair(
				new PairFlatMapFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;
			
			
			@Override
			public Iterable<Tuple2<String, Integer>> call(String t) throws Exception {
				List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
				for (String word : t.split(" ")) {
					list.add(new Tuple2<String, Integer>(word, 1));
				}
				return list;
			}
		});
		
		JavaPairDStream<String, Integer> resultDStream = flatMapToPair.reduceByKey(
				new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		//完成上面的逻辑后需要一个output operator
		resultDStream.print();
		javaStreamingContext.start();
		javaStreamingContext.awaitTermination();
		//这里没有SparkContext和JavaSparkContext
		javaStreamingContext.stop(false);
		
	}
}
