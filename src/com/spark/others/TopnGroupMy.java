package com.spark.others;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 分组取topN的问题， 如果我们用groupByKey()
 * 
 * 得到 （"sanban", [100, 86, 79, 79, 32, 78]） 如果相要取到每个班最大的三名同学的成绩， List
 * Collections.sort(list) 有什么问题？ 某一个key对应的value 有可能非常非常的多，放到list里面会有OOM的风险
 * 解决办法：定义一个定长的数组，通过一个简单的算法
 * 
 * @author ZORO
 *
 */
public class TopnGroupMy {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("secondSort");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		List<Tuple2<String, Integer>> scoreList = Arrays.asList(new Tuple2<String, Integer>("sanban", 78),
				new Tuple2<String, Integer>("sanban", 80), new Tuple2<String, Integer>("sanban", 199),
				new Tuple2<String, Integer>("sanban", 89), new Tuple2<String, Integer>("siban", 78),
				new Tuple2<String, Integer>("siban", 23), new Tuple2<String, Integer>("siban", 79),
				new Tuple2<String, Integer>("siban", 68));
		
		/**
		 * 输出结果sanban : 199 89 80    siban : 79 78 68
		 */
		JavaPairRDD<String, Integer> banjiScoreRDD = jsc.parallelizePairs(scoreList);

		JavaPairRDD<String, Iterable<Integer>> groupByKeyRDD = banjiScoreRDD.groupByKey();
		
		groupByKeyRDD.mapToPair(
				new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> t)throws Exception {
						Integer[] val = new Integer[3];

						Iterator<Integer> iterator = t._2.iterator();
						while (iterator.hasNext()) {
							Integer score = iterator.next();
							for (int i = 0; i < val.length; i++) {
								if (val[i] == null) {
									val[i] = score;
									// 跳出当前for循环
									break;
								} else if (score > val[i]) {
									// 索引从0 -》 .length-1, 数值从大到小，如果大于前面的，往后面移动一位
									for (int j = (val.length - 1); j > i; j--) {
										val[j] = val[j - 1];
									}
									val[i] = score;
									break;
								}
							}

						}
						return new Tuple2<String, Iterable<Integer>>(t._1, Arrays.asList(val));
					}
				}).foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
						StringBuilder sb = new StringBuilder();
						sb.append(t._1 + " : ");
						Iterator<Integer> iterator = t._2.iterator();
						while(iterator.hasNext()){
							sb.append(iterator.next() + " ");
						}
						System.out.println(sb.toString().trim());
					}
				});
		
		jsc.stop();
	}

}
