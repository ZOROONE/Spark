package com.spark.control;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class PersistControl {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("cache");

		JavaSparkContext js = new JavaSparkContext(conf);
		List<Tuple2<String, String>> pairs1 = Arrays.asList(new Tuple2<String, String>("beijing", "QH"),
				new Tuple2<String, String>("beijing", "BD"), new Tuple2<String, String>("shanghai", "FD"));

		JavaPairRDD<String, String> pairs1RDD = js.parallelizePairs(pairs1);

		/**
		 * 可以指定持久化的级别。最常用的是MEMORY_ONLY和MEMORY_AND_DISK。”_2”表示有副本数。
		 * 
		 * 1.cache和persist都是懒执行，必须有一个action类算子触发执行。
		 * 2.cache和persist算子的返回值可以赋值给一个变量，在其他job中直接使用这个变
		 *     量就是使用持久化的数据了。持久化的单位是partition。 
		 * 3. cache和persist算子后不能立即紧跟action算子。
		 *     错误：rdd.cache().count() 返回的不是持久化的RDD，而是一个数值了。
		 */
		JavaPairRDD<String, String> persistRDD = pairs1RDD.persist(StorageLevel.MEMORY_AND_DISK());
		persistRDD.count();

		js.close();
	}
}
