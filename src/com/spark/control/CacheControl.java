package com.spark.control;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class CacheControl {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("cache");

		JavaSparkContext js = new JavaSparkContext(conf);
		List<Tuple2<String, String>> pairs1 = Arrays.asList(new Tuple2<String, String>("beijing", "QH"),
				new Tuple2<String, String>("beijing", "BD"), new Tuple2<String, String>("shanghai", "FD"));

		JavaPairRDD<String, String> pairs1RDD = js.parallelizePairs(pairs1);
		
		// 默认默认将RDD的数据持久化到内存中。cache是懒执行。
		// 注意：chche () = persist()=persist(StorageLevel.Memory_Only)
		JavaPairRDD<String, String> cacheRDD = pairs1RDD.cache();
		
		cacheRDD.count();
		
		js.close();
		
	}

}
