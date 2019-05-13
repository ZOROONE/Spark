package com.spark.transformation;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class CogroupTransform {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapAbout");

		JavaSparkContext js = new JavaSparkContext(conf);
		List<Tuple2<String, String>> pairs1 = Arrays.asList(new Tuple2<String, String>("beijing", "QH"),
				new Tuple2<String, String>("beijing", "BD"), new Tuple2<String, String>("shanghai", "FD"));
		List<Tuple2<String, String>> pairs2 = Arrays.asList(new Tuple2<String, String>("beijing", "BJJT"),
				new Tuple2<String, String>("shanghai", "SHJT"), new Tuple2<String, String>("shanghai", "SH"));

		JavaPairRDD<String, String> pairs1RDD = js.parallelizePairs(pairs1);
		JavaPairRDD<String, String> pairs2RDD = js.parallelizePairs(pairs2);

		/**
		 * cogroup:
		 * 当调用类型（K,V）和（K，W）的数据上时，返回一个数据集（K，（Iterable<V>,Iterable<W>））,
		 * 相当于groupWith，与join不同的是，join会产生笛卡尔积
		 * 
		 */
		JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> cogroupRDD = pairs1RDD.cogroup(pairs2RDD);
		//和上面一样
//		JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> groupWithRDD = pairs1RDD.groupWith(pairs2RDD);
		
		/**
		 * 几个key, 几条数据
		 * key : beijing	pairs1RDD : QH BD 	pairs2RDD : BJJT 
		 * key : shanghai	pairs1RDD : FD 	pairs2RDD : SHJT SH
		 */
		cogroupRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> tuple) throws Exception {
				StringBuilder sb = new StringBuilder();
				sb.append("key : " + tuple._1 + "\t" + "pairs1RDD : ");
				Iterator<String> iterator1 = tuple._2._1.iterator();
				while (iterator1.hasNext()) {
					sb.append(iterator1.next() + " ");
				}
				sb.append("\t" + "pairs2RDD : ");
				Iterator<String> iterator2 = tuple._2._2.iterator();
				while (iterator2.hasNext()) {
					sb.append(iterator2.next() + " ");
				}

				System.out.println(sb.toString());
			}
		});
		js.close();
	}
}
