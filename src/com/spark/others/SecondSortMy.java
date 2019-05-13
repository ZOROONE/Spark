package com.spark.others;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 所谓二次排序，就是自定义key类，然后自己实现compareTo方法，
 * 这个key, 可以设置多个属性，先按照，第一个属性比，然后在按照第二个属性比，
 * 
 * 
 * @author ZORO
 *
 */
public class SecondSortMy {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("secondSort");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		List<Tuple2<String, Integer>> asList = Arrays.asList(new Tuple2<String, Integer>("apple", 23),
				new Tuple2<String, Integer>("apple", 12), new Tuple2<String, Integer>("hive", 12),
				new Tuple2<String, Integer>("banaan", 10), new Tuple2<String, Integer>("hive", 3));

		JavaRDD<Tuple2<String, Integer>> fruiltPriceRDD = jsc.parallelize(asList);

		/**
		 *  (apple,12) 
		 *  (apple,23) 
		 *  (banaan,10) 
		 *  (hive,3)
		 *  (hive,12)
		 */
		fruiltPriceRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, FruiltPrice, Tuple2<String, Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<FruiltPrice, Tuple2<String, Integer>> call(Tuple2<String, Integer> t) throws Exception {
				FruiltPrice key = new FruiltPrice(t._1, t._2);

				return new Tuple2<SecondSortMy.FruiltPrice, Tuple2<String, Integer>>(key, t);
			}
		}).sortByKey().foreach(new VoidFunction<Tuple2<FruiltPrice, Tuple2<String, Integer>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<FruiltPrice, Tuple2<String, Integer>> t) throws Exception {
				System.out.println(t._2);
			}
		});

		jsc.stop();
	}

	/**
	 * 自定义二次排序类
	 * 
	 * @author ZORO
	 *
	 */
	static class FruiltPrice implements Serializable, Comparable<FruiltPrice> {
		private static final long serialVersionUID = 1L;
		private String fruilt;
		private Integer price;

		public String getFruilt() {
			return fruilt;
		}

		public FruiltPrice() {
			super();
		}

		public FruiltPrice(String fruilt, Integer price) {
			this.fruilt = fruilt;
			this.price = price;
		}

		public void setFruilt(String fruilt) {
			this.fruilt = fruilt;
		}

		public Integer getPrice() {
			return price;
		}

		public void setPrice(Integer price) {
			this.price = price;
		}

		/**
		 * 自己设置排序规则，先按照第一个字段比较，相同在按照第二个字段比较
		 */
		@Override
		public int compareTo(FruiltPrice o) {

			int compareToFruilt = this.getFruilt().compareTo(o.getFruilt());

			if (compareToFruilt != 0) {
				return compareToFruilt;
			}
			return this.getPrice().compareTo(o.getPrice());
		}
	}
}
