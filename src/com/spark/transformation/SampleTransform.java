package com.spark.transformation;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class SampleTransform {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapAbout");

		JavaSparkContext js = new JavaSparkContext(conf);
		List<Tuple2<String, String>> pairs1 = Arrays.asList(new Tuple2<String, String>("beijing", "QH"),
				new Tuple2<String, String>("beijing", "BD"), new Tuple2<String, String>("shanghai", "FD"),
				new Tuple2<String, String>("beijing", "BD"), new Tuple2<String, String>("shanghai", "FD"),
				new Tuple2<String, String>("beijing", "BD"), new Tuple2<String, String>("shanghai", "FD"));

		JavaPairRDD<String, String> pairs1RDD = js.parallelizePairs(pairs1);
		/**
		 * false是不放回， 0.5是抽样比例, 并不是严格按照比例，只是大概
		 *  (beijing,QH) (beijing,BD)(beijing,BD)
		 */
		JavaPairRDD<String, String> sampleRDD = pairs1RDD.sample(false, 0.5);
		sampleRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, String> t) throws Exception {
				System.out.println(t);
			}
		});

		js.close();
	}
}
