package com.spark.action;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TakeAction {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		//conf.setMaster("local");
		conf.setAppName("TakeAction");

		JavaSparkContext js = new JavaSparkContext(conf);
		JavaRDD<String> lineRDD = js.parallelize(Arrays.asList("apple", "hadoop", "hadoop"));
		
		// take(1) 相当于first
		List<String> take = lineRDD.take(1);
		for (int i = 0; i < take.size(); i++) {
			//apple
			System.out.println(take.get(i));
		}
		
		js.stop();
	}

}
