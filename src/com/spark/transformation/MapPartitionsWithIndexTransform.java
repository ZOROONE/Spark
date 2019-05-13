package com.spark.transformation;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

public class MapPartitionsWithIndexTransform {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapAbout");

		JavaSparkContext js = new JavaSparkContext(conf);
		JavaRDD<String> lineRDD = js.parallelize(Arrays.asList("apple banbana", "blue hadoop", "hive hadoop apple"));
		
		/**
		 * 与mapPartitions 基本一样，只不过多了一个partition索引
		 */
		JavaRDD<String> mapPartitionsWithIndexRDD = lineRDD
				.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<String> call(Integer index, Iterator<String> lines) throws Exception {

						System.out.println(index);

						List<String> result = new LinkedList<String>();
						while (lines.hasNext()) {
							// 用正则匹配
							String[] words = Pattern.compile("[ ]").split(lines.next());
							result.addAll(Arrays.asList(words));
						}

						return result.iterator();
					}
				}, false);

		/**
		 * 输出结果：每个单词一行，0代表一个分区， 目前只有一个分区
		 * 0 apple banbana blue hadoop hive hadoop apple
		 */
		mapPartitionsWithIndexRDD.foreach(new VoidFunction<String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String word) throws Exception {
				System.out.println(word);
			}
		});
		js.close();
	}
}
