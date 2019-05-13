package com.spark.transformation;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

public class FlatMapTransform {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapAbout");

		JavaSparkContext js = new JavaSparkContext(conf);
		JavaRDD<String> lineRDD = js.parallelize(Arrays.asList("apple banbana", "blue hadoop", "hive hadoop apple"));

		/**
		 * 进来一条数据，出去一条到多条数据
		 */
		JavaRDD<String> flatMapRDD = lineRDD.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(Pattern.compile("[ ]").split(line));
			}
		});

		/**
		 * 输出结果：一行一个 apple banbana blue hadoop hive hadoop apple
		 */
		flatMapRDD.foreach(new VoidFunction<String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String word) throws Exception {
				System.out.println(word);
			}
		});
		js.close();
	}
}
