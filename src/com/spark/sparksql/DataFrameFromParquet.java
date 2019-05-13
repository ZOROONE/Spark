package com.spark.sparksql;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class DataFrameFromParquet {
	
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
		
		//保存的两种方式
		nameDataFrame.write().mode(SaveMode.Overwrite).format("parquet").save("./data/parquet");
		//nameDataFrame.write().mode(SaveMode.Overwrite).parquet("./data/parquet");
		
		//读取的两种方式 orc格式的也支持
		DataFrame nameDF = sqlContext.read().format("parquet").load("./data/parquet");
		//DataFrame nameDF = sqlContext.read().parquet("./data/parquet");
	}
}
