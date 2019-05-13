package com.spark.action;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class ReduceAction {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapAbout");

		JavaSparkContext js = new JavaSparkContext(conf);
		JavaRDD<String> lineRDD = js.parallelize(Arrays.asList("apple", "hadoop", "hadoop"));
		
		/**
		 * 根据聚合逻辑聚合数据集中的每个元素。
		 */
		String reduce = lineRDD.reduce(new Function2<String, String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String v1, String v2) throws Exception {
				return v1 + "_" + v2;
			}
		});
		
		System.out.println(reduce);
		js.close();
		
	}
}
