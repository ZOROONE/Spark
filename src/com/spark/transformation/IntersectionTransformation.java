package com.spark.transformation;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class IntersectionTransformation {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapAbout");

		JavaSparkContext js = new JavaSparkContext(conf);
		JavaRDD<String> pairs1RDD = js.parallelize(Arrays.asList("a", "b", "c"));
		JavaRDD<String> pairs2RDD = js.parallelize(Arrays.asList("a", "e", "f"));
		
		/**
		 * intersection取两个RDD的交集
		 */
		JavaRDD<String> intersectionRDD = pairs1RDD.intersection(pairs2RDD);

		/**
		 * 数据是： a 一行一个
		 * 有shuffle
		 * 
		 */
		intersectionRDD.foreach(new VoidFunction<String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String tuple) throws Exception {
				System.out.println(tuple);
			}
		});

		js.close();
	}
}
