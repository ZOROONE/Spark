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

import com.google.common.base.Optional;

import scala.Tuple2;

/**
 * * UpdateStateByKey的主要功能:
 * 1、为Spark Streaming中每一个Key维护一份state状态，state类型可以是任意类型的， 
 * 可以是一个自定义的对象，那么更新函数也可以是自定义的。
 * 2、通过更新函数对该key的状态不断更新，对于每个新的batch而言，Spark Streaming会在
 * 使用updateStateByKey的时候为已经存在的key进行state的状态更新
 * 
 * hello,3
 * bjsxt,2
 * 
 * 如果要不断的更新每个key的state，就一定涉及到了状态的保存和容错，这个时候就需要开启checkpoint机制和功能 
 * 
 * 全面的广告点击分析
 *
 * 有何用？   统计广告点击流量，统计这一天的车流量，统计点击量
 * @author ZORO
 *
 */
public class UpdateStateByKey {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf();
		conf.setMaster("local[2]").setAppName("updateStateByKey");
		JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.seconds(5));
		/**
		 * 设置checkpoint目录
		 * 
		 * 多久会将内存中的数据（每一个key所对应的状态）写入到磁盘上一份呢？
		 * 	如果你的batch interval小于10s  那么10s会将内存中的数据写入到磁盘一份
		 * 	如果bacth interval 大于10s，那么就以bacth interval为准
		 * 
		 * 这样做是为了防止频繁的写HDFS
		 */
		javaStreamingContext.sparkContext().setCheckpointDir("./checkpointDir");
		
		//jsc.checkpoint("hdfs://node1:9000/spark/checkpoint");
 		//jsc.checkpoint("./checkpoint");
		
		JavaReceiverInputDStream<String> lineDStream = javaStreamingContext.socketTextStream("node001", 9999);
		JavaPairDStream<String, Integer> wordPairDStream = lineDStream.flatMapToPair(
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
		
		JavaPairDStream<String, Integer> updateStateByKeyDStream = wordPairDStream.updateStateByKey(
				new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Optional<Integer> call(List<Integer> v1, Optional<Integer> v2) throws Exception {
				
				/**
				 * v2 是以前的状态
				 * v1是这一个Duration内的DStream数据， 按照key进行groupbykey后的val  [1, 1, 1, 1]
				 * 更新state   取以前的值加这一次的值  一天的总销售额
				 */
				Integer val = 0;
				
				if(v2.isPresent()) {
					val = v2.get();
				}
				
				for (Integer num : v1) {
					val += num;
				}
				
				return Optional.of(val);
			}
		});
		
		//output 算子
		updateStateByKeyDStream.print();
		
		javaStreamingContext.start();
		javaStreamingContext.awaitTermination();
		javaStreamingContext.stop();
	}
}
