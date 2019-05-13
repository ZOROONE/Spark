package com.spark.others;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

public class BroadcastMy {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapAbout");

		JavaSparkContext js = new JavaSparkContext(conf);
		
		//需要被广播出去的过滤条件
		List<String> filterList = Arrays.asList("apple", "hadoop");
		final Broadcast<List<String>> broadcast = js.broadcast(filterList);
		
		
		List<String> word = Arrays.asList("apple", "hadoop", "hive", "spark");
		JavaRDD<String> wordRDD = js.parallelize(word);
		
		JavaRDD<String> filterRDD = wordRDD.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String v1) throws Exception {
				//取反，因为true才会通过
				return !broadcast.value().contains(v1);
			}
		});
		
		filterRDD.foreach(new VoidFunction<String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});
		
		//里面有js.close(), conf.close()
		js.stop();

	}
}
