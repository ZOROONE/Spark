package com.spark.others;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class PartitionerMy {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapAbout");

		JavaSparkContext js = new JavaSparkContext(conf);
		List<Tuple2<String, Integer>> pairs1 = Arrays.asList(new Tuple2<String, Integer>("zhangsan", 18),
				new Tuple2<String, Integer>("lisi", 27), new Tuple2<String, Integer>("wangwu", 19),
				new Tuple2<String, Integer>("tianqi", 8));

		JavaPairRDD<String, Integer> nameAgeRDD = js.parallelizePairs(pairs1, 2);

		/**
		 * 0 (zhangsan,18) (lisi,27) 1 (wangwu,19) (tianqi,8)
		 */
		nameAgeRDD
				.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<String, Integer>>, Iterator<String>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<String> call(Integer v1, Iterator<Tuple2<String, Integer>> v2) throws Exception {
						System.out.println(v1);
						while (v2.hasNext()) {
							System.out.println(v2.next());
						}
						List<String> list = new ArrayList<>();
						return list.iterator();
					}
				}, false).count();

		/**
		 * 0
		 * 1 (zhangsan,18) (lisi,27) (wangwu,19) (tianqi,8)
		 * 
		 */
		nameAgeRDD.partitionBy(new Partitioner() {
			private static final long serialVersionUID = 1L;

			// 指定分区个数
			@Override
			public int numPartitions() {
				return 2;
			}

			// 0 1分区, 根据自己传进来的key，判断他去哪个分区
			@Override
			public int getPartition(Object key) {
				return 1;
			}
		}).mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<String, Integer>>, Iterator<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Integer v1, Iterator<Tuple2<String, Integer>> v2) throws Exception {
				System.out.println(v1);
				while (v2.hasNext()) {
					System.out.println(v2.next());
				}
				List<String> list = new ArrayList<>();
				return list.iterator();
			}
		}, false).count();

		js.close();

	}
}
