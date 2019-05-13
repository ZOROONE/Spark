package com.spark.transformation;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class FlatMapToPairTransform {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapAbout");

		JavaSparkContext js = new JavaSparkContext(conf);
		JavaRDD<String> lineRDD = js.parallelize(Arrays.asList("apple banbana", "blue hadoop", "hive hadoop apple"));

		/**
		 * 进来一行数据，出去多行数据，类型是tuple2, 相当于flatMap + mapToPair
		 */
		JavaPairRDD<String, Integer> flatMapToPairRDD = lineRDD
				.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<String, Integer>> call(String line) throws Exception {
						List<Tuple2<String, Integer>> result = new LinkedList<Tuple2<String, Integer>>();
						String[] split = Pattern.compile("[ ]").split(line);
						for (int i = 0; i < split.length; i++) {
							result.add(new Tuple2<String, Integer>(split[0], 1));
						}

						return result;
					}
				});

		/**
		 * 输出结果： 一行一条
		 * (apple,1) (apple,1) (blue,1) (blue,1) (hive,1) (hive,1) (hive,1)
		 */
		flatMapToPairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> tuple) throws Exception {
				System.out.println(tuple);
			}
		});
		js.close();
	}
}
