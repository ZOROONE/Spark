package com.spark.transformation;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class GroupByTransform {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapAbout");

		JavaSparkContext js = new JavaSparkContext(conf);
		List<Tuple2<String, String>> pairs1 = Arrays.asList(new Tuple2<String, String>("beijing", "QH"),
				new Tuple2<String, String>("beijing", "BD"), new Tuple2<String, String>("shanghai", "FD"));

		JavaPairRDD<String, String> pairs1RDD = js.parallelizePairs(pairs1);
		
		/**
		 * 不一定非得应用在key,v格式的rdd上， groupByKey是groupBy的一种情况，即作用在K,V上面的
		 * groupBy, 自己根据里面的值，给出分组的key, 得到一个tuple2类型的数据，_1是key, _2是值
		 * 
		 */
		JavaPairRDD<String, Iterable<Tuple2<String, String>>> groupByRDD = pairs1RDD.groupBy(new Function<Tuple2<String, String>, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public String call(Tuple2<String, String> tuple) throws Exception {
						/**
						 * 入股我们想要按照key分组 , 共两条数据，因为key就两个
						 * beijing (beijing,QH) (beijing,BD)
						 * shanghai (shanghai,FD)
						 */
						// return tuple._1;
						/**
						 * 如果按照val分组，三条数据，因为三个val
						 * QH  (beijing,QH)
						 * BD  (beijing,BD)
						 * FD  (shanghai,FD)
						 */
						// return tuple._2;
						
						/**
						 * 还可以对这个数据，进行各种判断，最后给他指定一个key
						 * 下面不管什么值，我们都给他返回自定义，则分组就有一个组，
						 *  zidingyi (beijing,QH) (beijing,BD) (shanghai,FD)
						 */
						
						return "zidingyi";
						
					}
				});
		groupByRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Tuple2<String, String>>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Iterable<Tuple2<String, String>>> t) throws Exception {
				// 打印key值，即按什么分组的值
				System.out.println(t._1);
				Iterator<Tuple2<String, String>> iterator = t._2.iterator();
				// 打印一组中有哪些数据
				while (iterator.hasNext()) {
					System.out.println(iterator.next());

				}
			}
		});
		js.close();

	}
}
