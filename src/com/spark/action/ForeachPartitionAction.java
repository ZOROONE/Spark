package com.spark.action;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class ForeachPartitionAction {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapAbout");

		JavaSparkContext js = new JavaSparkContext(conf);
		JavaRDD<String> lineRDD = js.parallelize(Arrays.asList("apple", "hadoop", "hadoop"));
		
		/**
		 * 遍历的数据是每个partition的数据。
		 */
		lineRDD.foreachPartition(new VoidFunction<Iterator<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Iterator<String> t) throws Exception {
				System.out.println(t.toString());
			}
		});
		
		js.close();
	}

}
