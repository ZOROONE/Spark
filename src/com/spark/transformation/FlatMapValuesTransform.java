package com.spark.transformation;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class FlatMapValuesTransform {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapAbout");

		JavaSparkContext js = new JavaSparkContext(conf);
		JavaRDD<String> lineRDD = js.parallelize(Arrays.asList("apple banbana", "blue hadoop", "hive hadoop apple"));

		JavaPairRDD<String, Integer> mapToPairRDD = lineRDD.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String line) throws Exception {
				return new Tuple2<String, Integer>(line, 1);
			}
		});

		/**
		 * flatMapValues作用在key,v格式的RDD上，进来一个tuple(), 输出一到多个数据
		 * 
		 */
		JavaPairRDD<String, Integer> flatMapValues = mapToPairRDD
				.flatMapValues(new Function<Integer, Iterable<Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Integer> call(Integer v) throws Exception {

						return Arrays.asList(v, v, v);
					}
				});

		/**
		 * 输出结果：一行一个数据，膨胀了 
		 * (apple banbana,1) (apple banbana,1) (apple banbana,1) (blue
		 * hadoop,1) (blue hadoop,1) (blue hadoop,1) (hive hadoop apple,1) (hive
		 * hadoop apple,1) (hive hadoop apple,1)
		 */
		flatMapValues.foreach(new VoidFunction<Tuple2<String, Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> tuple) throws Exception {
				System.out.println(tuple);
			}
		});

		js.close();
	}
}
