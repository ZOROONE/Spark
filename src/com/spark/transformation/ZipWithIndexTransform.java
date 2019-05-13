package com.spark.transformation;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class ZipWithIndexTransform {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapAbout");

		JavaSparkContext js = new JavaSparkContext(conf);
		JavaRDD<String> lineRDD = js.parallelize(Arrays.asList("apple", "hadoop", "hadoop"));

		/**
		 * 该函数将RDD中的元素和这个元素在RDD中的索引号（从0开始）组合成（K,V）对。
		 */
		JavaPairRDD<String, Long> zipWithIndexRDD = lineRDD.zipWithIndex();

		/**
		 * (apple,0) (hadoop,1) (hadoop,2)
		 */
		zipWithIndexRDD.foreach(new VoidFunction<Tuple2<String, Long>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Long> t) throws Exception {
				System.out.println(t);
			}
		});

		js.close();
	}
}
