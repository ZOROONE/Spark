package com.spark.transformation;

import java.util.Arrays;
import java.util.Iterator;
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

public class MapPartitionsToPairTransform {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapAbout");

		JavaSparkContext js = new JavaSparkContext(conf);
		JavaRDD<String> lineRDD = js.parallelize(Arrays.asList("apple banbana", "blue hadoop", "hive hadoop apple"));

		/**
		 * 进来一个partition的数据，出去一个partition的数据，但是类型要是Tuple2 可以代替， flatMap +
		 * [filter] + mapToPair等多个算子的功能
		 */
		JavaPairRDD<String, Integer> mapPartitionsToPair = lineRDD
				.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<String, Integer>> call(Iterator<String> lines) throws Exception {

						List<Tuple2<String, Integer>> result = new LinkedList<Tuple2<String, Integer>>();

						while (lines.hasNext()) {
							String[] split = Pattern.compile("[ ]").split(lines.next());
							for (int i = 0; i < split.length; i++) {
								result.add(new Tuple2<String, Integer>(split[i], 1));
							}
						}

						return result;
					}
				});

		/**
		 * 输出结果：一行一个（）
		 * (apple,1) (banbana,1) (blue,1) (hadoop,1) (hive,1) (hadoop,1) (apple,1)
		 */
		mapPartitionsToPair.foreach(new VoidFunction<Tuple2<String, Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> tuple) throws Exception {
				System.out.println(tuple);
			}
		});
		
		js.close();

	}
}
