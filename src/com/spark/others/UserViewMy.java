package com.spark.others;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class UserViewMy {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("uv");

		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> lineRDD = jsc.textFile("hdfs://node001:8020/user/root/spark/data/logdata");

		/**
		 * 每个page 下的uv,  即每个page页面下有多少个用户访问， 
		 * 一个用户可能多次访问同一个page页面， 因此需要去重
		 * (15808,www.jd.com) 
		 * (15703,www.dangdang.com) 
		 * (15695,www.mi.com)
		 * (15684,www.suning.com) 
		 * (15679,www.gome.com.cn)
		 */

		List<Tuple2<Integer, String>> viewUserPageResult = lineRDD.map(new Function<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String line) throws Exception {
				String[] split = line.split("\t");
				// 提取ip_page， 同一个page下的user去重的效果，即可求出每个page下的pv

				// 如果求总pv, 直接以ip或者user唯一性为key，distinct直接可以求出
				return split[0] + "_" + split[5];
			}
		}).distinct().mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				// 返回一个 （page, 1）的RDD
				return new Tuple2<String, Integer>(t.split("_")[1], 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		}).mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
				return new Tuple2<Integer, String>(t._2, t._1);
			}
		}).sortByKey(false).take(5);

		for (int i = 0; i < viewUserPageResult.size(); i++) {
			System.out.println(viewUserPageResult.get(i));
		}

		jsc.stop();

	}

}
