package com.spark.transformation;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class ZipTransform {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapAbout");

		JavaSparkContext js = new JavaSparkContext(conf);
		JavaRDD<String> lineRDD = js.parallelize(Arrays.asList("apple", "hadoop", "hadoop"));
		JavaRDD<Integer> line2RDD = js.parallelize(Arrays.asList(23, 3, 5));

		/**
		 * 将两个RDD中的元素（KV格式/非KV格式）变成一个KV格式的RDD,两个RDD的个数必须相同。 即分区数相同，元素个数相同
		 */
		JavaPairRDD<String, Integer> zipRDD = lineRDD.zip(line2RDD);
		/**
		 * (apple,23) (hadoop,3) (hadoop,5)
		 */
		zipRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t);
			}
		});

		js.close();
	}

}
