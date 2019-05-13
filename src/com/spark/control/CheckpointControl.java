package com.spark.control;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class CheckpointControl {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("cache");

		JavaSparkContext js = new JavaSparkContext(conf);
		// 会在..目录创建一个文件夹
		js.setCheckpointDir("hdfs://node001:8020/user/root/spark/checkpoint");
		List<Tuple2<String, String>> pairs1 = Arrays.asList(new Tuple2<String, String>("beijing", "QH"),
				new Tuple2<String, String>("beijing", "BD"), new Tuple2<String, String>("shanghai", "FD"));
		/**
		 * 1. checkpoint将RDD持久化到磁盘，还可以切断RDD之间的依赖关系。
		 */
		JavaPairRDD<String, String> pairs1RDD = js.parallelizePairs(pairs1);

		pairs1RDD.checkpoint();
		pairs1RDD.count();

		js.close();
	}

}
