package com.spark.transformation;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class UnionTransform {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapAbout");

		JavaSparkContext js = new JavaSparkContext(conf);
		JavaRDD<String> pairs1RDD = js.parallelize(Arrays.asList("a", "b", "c"));
		JavaRDD<String> pairs2RDD = js.parallelize(Arrays.asList("a", "e", "f"));

		/**
		 * union两个RDD分区直接相加，数据相加，但是RDD类型要一致
		 */
		JavaRDD<String> unionRDD = pairs1RDD.union(pairs2RDD);

		/**
		 * 也证明了分区直接相加，没有shuffle
		 * 输出结果： a b c 第一个分区
		 * 输出结果： a e f 第二个分区
		 */
		unionRDD.foreach(new VoidFunction<String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String word) throws Exception {
				System.out.println(word);
			}
		});

		js.close();
	}
}
