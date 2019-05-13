package com.spark.sparkstreaming;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class DirectKafka {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf();
		conf.setMaster("local[*]").setAppName("Direct");

		// 反压机制，默认不开启
		// conf.set("spark.streaming.backpressure.enabled", "false");
		// conf.set("spark.streaming.kafka.maxRatePerPartition", "100");

		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

		/**
		 * 可以不设置checkpoint 不设置不保存offset,offset默认在内存中有一份，
		 * 如果设置checkpoint在checkpoint也有一份offset， 一般要设置。
		 */
		jsc.checkpoint("hdfs://node001:8020/kafka/offect/direct");

		Map<String, String> kafkaParameters = new HashMap<>();
		kafkaParameters.put("metadata.broker.list", "node001:9092,node002:9092,node003:9092");
		// kafkaParameters.put("auto.offset.reset", "smallest");
		Set<String> topicSet = new HashSet<>();
		topicSet.add("t0122");

		/**
		 * 第一个是JavaStreamingContext jsc 
		 * 第二个String是key的类型，
		 * 第三个String是val的类型
		 * 第四个对应着第key类型的解码，
		 * 第五个对应着val的解码 
		 * 第六个是kafka的参数，metadata.broker.list kafka集群列表（Direct是将kafka当成存储数据看待）
		 *  第七个是topics的列表，可以有多个topic
		 */
		JavaPairInputDStream<String, String> keyValDStream = KafkaUtils.createDirectStream(
				jsc, String.class,String.class, 
				StringDecoder.class, StringDecoder.class, kafkaParameters, topicSet);

		/**
		 * 并行度: 1、linesDStram里面封装到的是RDD， RDD里面有partition与读取topic的parititon数是一致的。
		 * 2、从kafka中读来的数据封装一个DStram里面，可以对这个DStream重分区 reaprtitions(numpartition)
		 */
		//JavaPairDStream<String, String> repartition = keyValDStream.repartition(10);
		
		
		JavaPairDStream<String, Integer> actionDStream = keyValDStream
				.mapToPair(new PairFunction<Tuple2<String, String>, String, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(Tuple2<String, String> t) throws Exception {
						return new Tuple2<String, Integer>(t._2.split("\t")[5], 1);
					}
				});

		JavaPairDStream<String, Integer> resultDStream = actionDStream
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
				});

		resultDStream.print();
		jsc.start();
		jsc.awaitTermination();
		jsc.stop();
	}
}
