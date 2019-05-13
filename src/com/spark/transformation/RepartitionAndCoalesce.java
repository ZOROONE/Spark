package com.spark.transformation;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RepartitionAndCoalesce {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapAbout");

		JavaSparkContext js = new JavaSparkContext(conf);
		JavaRDD<String> lineRDD = js.parallelize(Arrays.asList("apple", "hadoop", "hadoop"), 3);
		
		/**
		 * repartition 增加或减少分区。会产生shuffle。（多个分区分到一个分区不会产生shuffle）
		 * 底层调用 coalesce(num, true)
		 */
		JavaRDD<String> repartitionRDD = lineRDD.repartition(2);
		
		/**
		 * coalesce常用来减少分区，第二个参数是减少分区的过程中是否产生shuffle。
		 * 因为增加分区，设置为false也会产生shuffle
		 */
		JavaRDD<String> coalesceRDD = lineRDD.coalesce(2, false);
		
		System.out.println(repartitionRDD.getNumPartitions());
		System.out.println(coalesceRDD.getNumPartitions());
		js.close();
		
	}
}
