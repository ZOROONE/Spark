package com.spark.sparkstreaming;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.base.Optional;

import scala.Tuple2;

public class Transform {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf();
		conf.setMaster("local[2]").setAppName("transform");
		JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.seconds(5));
		
		//每隔5秒就动态从文件中读取一次，实现动态更新黑名单
		final JavaPairRDD<String, Boolean> blackRDD = javaStreamingContext.sparkContext().parallelizePairs(Arrays.asList(
				new Tuple2<String, Boolean>("zhangsan", true)));
		
		//获得数据DStream
		JavaReceiverInputDStream<String> lineDStream = javaStreamingContext.socketTextStream("node001", 9999);
		
		JavaDStream<String> resultDStream = lineDStream.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public JavaRDD<String> call(JavaRDD<String> lineRDD) throws Exception {
				/**
				 * 这里的代码也是在Driver端执行的，上面每隔5秒来一个批次，也就是每隔5秒读一个文件，动态改变广播变量
				 * 
				 * 传进来的lineRDD是lineDStream里面封装的RDD
				 * 
				 * 1. lineRDD的数据格式是   "1 zhangsan"   "2 lisi"   ....
				 * 2. 首先将lineRDD转换成pair类型的RDD,  <"zhangsan", "1 zhangsan">    pairRDD
				 * 3. 将上一步的pairRDD与黑名单blackRDD进行左外连接  leftOuterJoin   <"zhangsan"， <"1 zhangsan", Optional<Boolean>>>
				 * 4. 将上一步得到的算子进行filter过滤，首先判断t._2._2是否存在，存在再接着判断是否为true，是则返回fasle，pass掉
				 * 5. 将上一步的到结果进行map算子，得到一个类型为String 的RDD 
				 */
				
				//第一步
				JavaPairRDD<String, String> pairRDD = lineRDD.mapToPair(new PairFunction<String, String, String>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Tuple2<String, String> call(String t) throws Exception {
						return new Tuple2<String, String>(t.split(" ")[0], t);
					}
				});
				
				//第二步
				JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> leftOuterJoin = pairRDD.leftOuterJoin(blackRDD);
				
				//第三步
				JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filter = leftOuterJoin.filter(
						new Function<Tuple2<String,Tuple2<String,Optional<Boolean>>>, Boolean>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> v1) throws Exception {
						if(v1._2._2.isPresent()) {
							return !v1._2._2.get();
						}
						
						return true;
					}
				});
				
				//第四部
				JavaRDD<String> resultRDD = filter.map(
						new Function<Tuple2<String,Tuple2<String,Optional<Boolean>>>, String>() {
					private static final long serialVersionUID = 1L;
					@Override
					public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> v1) throws Exception {
						return v1._2._1;
					}
				});
				
				//第五步
				return resultRDD;
			}
		});
		
		
		//outPut算子
		resultDStream.print();
		javaStreamingContext.start();
		javaStreamingContext.awaitTermination();
		javaStreamingContext.stop(true);
	}
}
