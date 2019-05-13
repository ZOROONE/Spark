package com.spark.transformation;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

public class MapPartitionsTransformation {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapPartitionsTransformation");

		JavaSparkContext js = new JavaSparkContext(conf);
		JavaRDD<String> lineRDD = js.parallelize(Arrays.asList("apple banbana", "blue hadoop", "hive hadoop apple"));
		
		/**
		 * mapPartitions与map算子比较是高效率算子，一次将一个分区数据传进来，传出去一个集合，
		 * 
		 * 可以间接替代，map , flatMap, filter, 等算子的功能
		 */
		JavaRDD<String> mapPartitionsRDD = lineRDD.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(Iterator<String> lines) throws Exception {
				List<String> result = new LinkedList<String>();
				while (lines.hasNext()) {
					// 这里我们不用String.split的方式，用正则表达式的切割方式
					String[] split = Pattern.compile("[ ]").split(lines.next());
					// 直接添加集合
					result.addAll(Arrays.asList(split));
				}
				return result;
			}
		});

		/**
		 * 输出结果一共多行，每行一个单词
		 * apple banbana blue hadoop hive hadoop apple
		 */
		mapPartitionsRDD.foreach(new VoidFunction<String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String word) throws Exception {
				System.out.println(word);
			}
		});

		js.close();
	}
}
