package com.spark.transformation;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class DIstinctTransform {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapAbout");

		JavaSparkContext js = new JavaSparkContext(conf);
		JavaRDD<String> lineRDD = js.parallelize(Arrays.asList("apple", "hadoop", "hadoop"));
		
		/**
		 * 去重， 相当于scala 里面的 map + reduceByKey + map java里面的mapToPair +
		 * reduceByKey + map
		 * 
		 */
		JavaRDD<String> distinctRDD = lineRDD.distinct();
		
		
		/**
		 * 结果和上面distinct是一样的
		 */
		lineRDD.mapToPair(new PairFunction<String, String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(String line) throws Exception {
				return new Tuple2<String, String>(line, null);
			}
		}).reduceByKey(new Function2<String, String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String before, String then) throws Exception {
				return null;
			}
		}).map(new Function<Tuple2<String,String>, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String call(Tuple2<String, String> tuple) throws Exception {
				return tuple._1;
			}
		});
		
		
		/**
		 * 输出结果：hadoop apple
		 */
		distinctRDD.foreach(new VoidFunction<String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String line) throws Exception {
				System.out.println(line);
			}
		});

		js.close();
	}
}
