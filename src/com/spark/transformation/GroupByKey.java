package com.spark.transformation;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class GroupByKey {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapAbout");

		JavaSparkContext js = new JavaSparkContext(conf);
		List<Tuple2<String, String>> pairs1 = Arrays.asList(new Tuple2<String, String>("beijing", "QH"),
				new Tuple2<String, String>("beijing", "BD"), new Tuple2<String, String>("shanghai", "FD"));

		JavaPairRDD<String, String> pairs1RDD = js.parallelizePairs(pairs1);
		
		/**
		 * 按照key分组
		 */
		JavaPairRDD<String, Iterable<String>> groupByKeyRDD = pairs1RDD.groupByKey();

		/**
		 * 结果： beijing [QH, BD] shanghai [FD]
		 */
		groupByKeyRDD.foreach(new VoidFunction<Tuple2<String, Iterable<String>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Iterable<String>> t) throws Exception {
				System.out.println(t._1);
				System.out.println(t._2.toString());
			}
		});

		js.close();

	}
}
