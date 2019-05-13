package com.spark.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

/**
 * 转换成JavaRDD
 * @author ZORO
 *
 */
public class DataFrameToJavaRDD {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("DataFrameToJavaRDD");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(jsc);
		
		DataFrame nameAgeDataFrame = sqlContext.read().json("./data/SparkSql/json");
		
		JavaRDD<Row> javaRDD = nameAgeDataFrame.javaRDD();
		javaRDD.mapToPair(new PairFunction<Row, String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Row row) throws Exception {
				// 这种根据字段索引方式取内容不推荐，因为容易出错
				//row.get(0);
				
				String name = String.valueOf(row.getAs("name"));
				//如果age null, 则这里应该也不会报错
				Object ageString = row.getAs("age");
				int age = 1000;
				if(ageString != null) {
					age = Integer.valueOf(ageString.toString());
				}
				
				return new Tuple2<String, Integer>(name, age);
			}
		}).foreach(new VoidFunction<Tuple2<String,Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t);
			}
		});
		
		jsc.close();
		
		
		
	}
}
