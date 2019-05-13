package com.spark.transformation;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class SampleByKeyTransform {
	
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
		 * 往map里面添加，自己想要对哪些key抽样，以及比例
		 */
		Map<String, Object> fractions = new HashMap<>();
		fractions.put("beijing", 0.2);
		fractions.put("shanghai", 0.6);
		//false是不放回抽样
		JavaPairRDD<String, String> sampleByKeyRDD = pairs1RDD.sampleByKey(false, fractions);
		//10是种子，保证同一份数据抽取出来的一样
		pairs1RDD.sampleByKey(true, fractions, 10);
		
		sampleByKeyRDD.foreach(new VoidFunction<Tuple2<String,String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, String> t) throws Exception {
				System.out.println(t);
			}
		});
		
		js.close();
	}
	
}
