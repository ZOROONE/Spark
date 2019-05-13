package com.spark.action;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class CollectAsMap {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapAbout");

		JavaSparkContext js = new JavaSparkContext(conf);
		List<Tuple2<String, String>> pairs1 = Arrays.asList(new Tuple2<String, String>("beijing", "QH"),
				new Tuple2<String, String>("beijing", "BD"), new Tuple2<String, String>("shanghai", "FD"));

		JavaPairRDD<String, String> pairs1RDD = js.parallelizePairs(pairs1);
		
		/**
		 * 作用在key，v格式的rdd上
		 * 如果有多个key相同，则map里面只有一个key, 覆盖掉
		 */
		Map<String, String> collectAsMap = pairs1RDD.collectAsMap();
		Iterator<Entry<String, String>> iterator = collectAsMap.entrySet().iterator();
		
		while(iterator.hasNext()){
			Entry<String, String> next = iterator.next();
			System.out.println(next.getKey() + "\t" + next.getValue());
		}
		
		js.close();
		
	}

}
