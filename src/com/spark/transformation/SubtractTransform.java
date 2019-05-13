package com.spark.transformation;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class SubtractTransform {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapAbout");

		JavaSparkContext js = new JavaSparkContext(conf);
		JavaRDD<String> pairs1RDD = js.parallelize(Arrays.asList("a", "b", "c"));
		JavaRDD<String> pairs2RDD = js.parallelize(Arrays.asList("a", "e", "f"));

		/**
		 * pairs1RDD - pairs2RDD pairs1RDD 去除pairs2RDD里面的内容
		 * 可以理解为，把pairs2RDD看成一个集合，每次对pairs1RDD读取一条数据，
		 * 然后看看pairs2RDD这个集合里面是否包含这条数据，包含则不输出，不包含则输出
		 */
		JavaRDD<String> subtractRDD = pairs1RDD.subtract(pairs2RDD);
		/**
		 * 运行结果： b c
		 */
		subtractRDD.foreach(new VoidFunction<String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String tuple) throws Exception {
				System.out.println(tuple);
			}
		});
		js.close();

	}
}
