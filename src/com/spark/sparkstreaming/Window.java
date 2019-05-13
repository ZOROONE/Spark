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

/**
 * 基于滑动窗口的热点搜索词实时统计
 * @author Zoro
 */
public class Window {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setMaster("local").setAppName("window");
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
		
		//设置日志级别为WARN
		jsc.sparkContext().setLogLevel("WARN");
		
		/**
		 * 未优化的窗口操作可以不设置checkpoint
		 * 优化的窗口操作需要设置checkpoint， 因为需要保存状态
		 */
		jsc.checkpoint("./checkpoint");
		//jsc.checkpoint("hdfs://node001:9000/spark/checkpoint");
		
		JavaReceiverInputDStream<String> lineDStream = jsc.socketTextStream("node001", 9999);
		
		//返回（word, 1）格式的DStream
		JavaPairDStream<String, Integer> wordDStream = lineDStream.flatMapToPair(
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
		
		/**
		 * 未优化的窗口操作:
		 * 每隔10秒，计算最近60秒内的数据，那么这个窗口大小就是60秒，里面有12个rdd，在没有计算之前，这些rdd是不会进行计算的。
		 * 那么在计算的时候会将这12个rdd聚合起来，然后一起执行reduceByKeyAndWindow操作 ，
		 * reduceByKeyAndWindow是针对窗口操作的而不是针对DStream操作的。
		 * 这李的窗口大小是15秒，滑动间距是5秒
		 */
		wordDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		}, Durations.seconds(15), Durations.seconds(5));
		
		
		/**
		 * 优化的窗口操作
		 * 第一个参数函数传进来的v1是前一个Window的该key的值，v2是新进来Duration的key的值
		 * 第二个参数函数传进来的v1是上一个参数处理后的值，v2是该window需要滑出的Duration的值，减去就是该次window的值
		 * 因为需要保存上一次window的状态，所以需要checkpoint
		 */
		JavaPairDStream<String, Integer> resultDStream = wordDStream.reduceByKeyAndWindow(
				new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		}, new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 - v2;
			}
		}, Durations.seconds(15), Durations.seconds(5));
		
		resultDStream.print();
		jsc.start();
		jsc.awaitTermination();
		jsc.stop();
		
	}
}
