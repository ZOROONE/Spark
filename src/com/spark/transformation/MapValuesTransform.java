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

public class MapValuesTransform {

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
		 * mapValues是作用在(key,v)格式的RDD上一个一个的遍历v
		 */
		JavaPairRDD<String, Integer> mapValuesRDD = mapToPairRDD.mapValues(new Function<Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer val) throws Exception {
				return val + 2;
			}
		});

		/**
		 * 输出结果：一行一个数据(apple banbana,3) (blue hadoop,3) (hive hadoop apple,3)
		 */
		mapValuesRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> tuple2) throws Exception {
				System.out.println(tuple2);
			}
		});

		js.close();
	}
}
