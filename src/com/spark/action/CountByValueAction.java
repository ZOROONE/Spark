package com.spark.action;

import java.util.Arrays;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class CountByValueAction {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapAbout");

		JavaSparkContext js = new JavaSparkContext(conf);
		JavaRDD<String> lineRDD = js.parallelize(Arrays.asList("apple", "hadoop", "hadoop"));
		
		/**
		 * value并不是代表k,v格式的v, 而是代表整体v,即并不一定作用在k,v格式的rdd上
		 *  {hadoop=2, apple=1}
		 */
		Map<String, Long> countByValueMap = lineRDD.countByValue();
		
		System.out.println(countByValueMap.toString());
		
		js.close();
	}
}
