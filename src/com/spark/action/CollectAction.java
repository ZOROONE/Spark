package com.spark.action;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class CollectAction {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapAbout");

		JavaSparkContext js = new JavaSparkContext(conf);
		JavaRDD<String> lineRDD = js.parallelize(Arrays.asList("apple", "hadoop", "hadoop"));
		
		//慎用，容易造成oom
		List<String> collect = lineRDD.collect();
		for (int i = 0; i < collect.size(); i++) {
			System.out.println(collect.get(i));
		}
		js.close();
	}

}
