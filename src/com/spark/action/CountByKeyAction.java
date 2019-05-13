package com.spark.action;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class CountByKeyAction {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapAbout");

		JavaSparkContext js = new JavaSparkContext(conf);
		List<Tuple2<String, String>> pairs1 = Arrays.asList(new Tuple2<String, String>("beijing", "QH"),
				new Tuple2<String, String>("beijing", "BD"), new Tuple2<String, String>("shanghai", "FD"));

		JavaPairRDD<String, String> pairs1RDD = js.parallelizePairs(pairs1);
		
		/**
		 *  作用到K,V格式的RDD上，根据Key计数相同Key的数据集元素
		 *  {beijing=2, shanghai=1}
		 */
		Map<String, Object> countByKeyRDD = pairs1RDD.countByKey();
		
		System.out.println(countByKeyRDD.toString());
		js.close();
	}

}
