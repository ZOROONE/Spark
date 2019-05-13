package com.spark.others;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class AccumulatorMy {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapAbout");

		JavaSparkContext js = new JavaSparkContext(conf);
		
		/**
		 * 累加器在Driver端定义赋初始值，累加器只能在Driver端读取，在Excutor端更新。
		 */
		final Accumulator<Integer> accumulator = js.accumulator(0);
		
		List<String> word = Arrays.asList("apple", "hadoop", "hive", "hadoop");
		JavaRDD<String> wordRDD = js.parallelize(word);
		
		wordRDD.foreach(new VoidFunction<String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String t) throws Exception {
				if(t.equals("hadoop")){
					accumulator.add(1);
				}
			}
		});
		
		System.out.println(accumulator.value());
		js.stop();
	}
}
