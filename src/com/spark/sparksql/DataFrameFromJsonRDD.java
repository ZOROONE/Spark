package com.spark.sparksql;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class DataFrameFromJsonRDD {
	
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster("local");
		sparkConf.setAppName("fromJsonRDD");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(jsc);
		
		
		JavaRDD<String> nameRDD = jsc.parallelize(Arrays.asList(
				"{\"name\":\"zhangsan\", \"age\":20}",
				"{\"name\":\"lisi\", \"age\":19}",
				"{\"name\":\"wangwu\", \"age\":18}"
				));
		JavaRDD<String> scoreRDD = jsc.parallelize(Arrays.asList(
				"{\"name\":\"zhangsan\", \"score\":90}",
				"{\"name\":\"lisi\", \"score\":80}",
				"{\"name\":\"wangwu\", \"score\":70}"
				));
		
		//name是String， age是long
		DataFrame nameDataFrame = sqlContext.read().json(nameRDD);
		DataFrame scoreDataFrame = sqlContext.read().json(scoreRDD);
		
		nameDataFrame.registerTempTable("Person");
		scoreDataFrame.registerTempTable("Study");
		
		
		DataFrame result = sqlContext.sql(
				"select p.name, p.age, s.score from Person p join Study s on (p.name = s.name)");
		result.show();
		
		
		jsc.stop();
	}
}
