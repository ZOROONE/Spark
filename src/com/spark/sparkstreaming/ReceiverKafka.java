package com.spark.sparkstreaming;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

//receiver 模式并行度是由blockInterval决定的
public class ReceiverKafka {
	
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local[2]").setAppName("receiver");
		//开启预写日志 WAL机制
		conf.set("spark.streaming.receiver.writeAheadLog.enable","true");
		
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
		
		//设置持久化目录
		//jsc.checkpoint("hdfs://node001:8020/kafka/receiver/checkpointer");
		jsc.checkpoint("./checkpointer");
		
		//设置读取的topic和接受数据的线程数
		Map<String, Integer> topicConsumerCurrency = new HashMap<String, Integer>();
		topicConsumerCurrency.put("t0122", 1);
		
		/**
		 * 第一个参数是StreamingContext
		 * 第二个参数是ZooKeeper集群信息（接受Kafka数据的时候会从Zookeeper中获得Offset等元数据信息）
		 * 第三个参数是Consumer Group 消费者组
		 * 第四个参数是消费的Topic以及并发读取Topic中Partition的线程数
		 * 
		 * 注意：
		 * KafkaUtils.createStream 使用五个参数的方法，设置receiver的存储级别
		 * 默认为MEMORY_AND_DISK_SER_2，开启WAL（备份到HDFS）后，需降为MEMORY_AND_DISK
		 */
		/*JavaPairReceiverInputDStream<String, String> createStream = KafkaUtils.createStream(
				jsc, "node001:2181,node002:2181,node003:2181", 
				"myFirstGroup", topicConsumerCurrency, 
				StorageLevel.MEMORY_AND_DISK());*/
		
		JavaPairReceiverInputDStream<String, String> lineDStream = KafkaUtils.createStream(
				jsc, "node001:2181,node002:2181,node003:2181", "myFirstGroup", topicConsumerCurrency);
		
		JavaPairDStream<String, Integer> flatMapToPair = lineDStream.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String,String>, String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, Integer>> call(Tuple2<String, String> t) throws Exception {
				List<Tuple2<String, Integer>> list = new ArrayList<>();
				list.add(new Tuple2<String, Integer>(t._2.split(" ")[5], 1));
				return list;
			}
		});
		
		flatMapToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		}).print();
		
		jsc.start();
		jsc.awaitTermination();
		jsc.stop();
	}
}
