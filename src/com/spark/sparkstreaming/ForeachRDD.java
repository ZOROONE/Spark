package com.spark.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class ForeachRDD {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local[2]");
		conf.setAppName("foreachRDD");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaStreamingContext javaStreamingContext = new JavaStreamingContext(jsc, Durations.seconds(5));
		
		JavaReceiverInputDStream<String> lineDStream = javaStreamingContext.socketTextStream("node001", 9999);
		//遍历RDD
		lineDStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<String> t) throws Exception {
				//这里的代码是在Driver端执行
				
				t.foreach(new VoidFunction<String>() {
					private static final long serialVersionUID = 1L;
					@Override
					public void call(String t) throws Exception {
						System.out.println(t);
					}
				});
			}});
		
		javaStreamingContext.start();
		javaStreamingContext.awaitTermination();
		//不加false关闭里面的sc jsc
		javaStreamingContext.stop();
	}
}
