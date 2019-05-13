package com.spark.others;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * 每个每个page下的pv, 即每个页面被访问多少次
 * @author ZORO
 *
 */
public class PageViewMy {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("pv");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaRDD<String> lineRDD = jsc.textFile("hdfs://node001:8020/user/root/spark/data/logdata/pvuvdata");
		JavaPairRDD<String, Integer> pageViewRDD = lineRDD.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String line) throws Exception {
				// 104.119.184.90 上海 2018-02-05 1517801736267	5688534602665062011 www.taobao.com Login
				//数据以制表符分割， 要pv, 所以要用www.taobao.cm
				String[] split = Pattern.compile("[\t]").split(line);

				return new Tuple2<String, Integer>(split[5], 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});

		List<Tuple2<Integer, String>> viewPageResult = pageViewRDD
				.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
					// 先进行k,v反转， 否则排序有问题
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
						return new Tuple2<Integer, String>(t._2, t._1);
					}
				}).sortByKey(false).take(5);

		/**
		 * (18835,www.jd.com) 
		 * (18716,www.dangdang.com) 
		 * (18690,www.mi.com)
		 * (18671,www.gome.com.cn) 
		 * (18618,www.suning.com)
		 */
		for (int i = 0; i < viewPageResult.size(); i++) {
			System.out.println(viewPageResult.get(i));
		}

		jsc.stop();
	}
}
