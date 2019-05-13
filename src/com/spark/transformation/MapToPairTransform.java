package com.spark.transformation;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class MapToPairTransform {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapAbout");

		JavaSparkContext js = new JavaSparkContext(conf);
		JavaRDD<String> lineRDD = js.parallelize(Arrays.asList("apple banbana", "blue hadoop", "hive hadoop apple"));

		/**
		 * 进来不管什么类型的数据， 出去一个Tuple2类型的数据
		 */
		JavaPairRDD<String, Integer> mapToPairRDD = lineRDD.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String line) throws Exception {
				// 直接出去一个二元组，可以前面用flatMap等算子先处理
				return new Tuple2<String, Integer>(line, 1);
			}
		});

		/**
		 * 输出结果：一行一个 (apple banbana,1) (blue hadoop,1) (hive hadoop apple,1)
		 */
		mapToPairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> tuple) throws Exception {
				System.out.println(tuple);

			}
		});
		js.close();
	}
}
