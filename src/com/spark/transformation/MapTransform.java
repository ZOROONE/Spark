package com.spark.transformation;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class MapTransform {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("MapAbout");
		
		JavaSparkContext js = new JavaSparkContext(conf);
		JavaRDD<String> lineRDD = js.parallelize(Arrays.asList("apple banbana", "blue hadoop", "hive hadoop apple"));
		
		/**
		 * map 输入一条，输出一条，输出类型自己指定
		 * 入股有hadoop,将里面的hadoop换成spark
		 * 如果没有 就什么都不输出，过滤掉
		 */
		JavaRDD<String> mapRDD = lineRDD.map(new Function<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String line) throws Exception {
				if(line.contains("hadoop")){
					return line.replace("hadoop", "spark");
				}
				return null;
			}
		});
		/**
		 * foreach是action算子
		 * 遍历结果：
		 * null
		 * blue spark
		 * hive spark apple
		 */
		mapRDD.foreach(new VoidFunction<String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(String line) throws Exception {
				System.out.println(line);
			}
		});
		js.close();
		
	}
}
