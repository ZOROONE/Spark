package com.spark.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

/**
 * spark on hive
 * 要想在提交到集群运行，看笔记
 * 
 * @author ZORO
 *
 */
public class SparkSqlOnHiveLocal {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf(true);
		conf.setMaster("local");
		conf.setAppName("sparkonhive");
		
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		/**
		 * hiveContext是sqlContext的子类,本地没有要提交到集群运行
		 */
		HiveContext hiveContext = new HiveContext(jsc);
		hiveContext.sql("show databases");
		hiveContext.sql("use hivetestdb");
		DataFrame sqlDataFrame = hiveContext.sql("select name, age from psn9 limit 5");
		sqlDataFrame.show();
		
		/**
		 * 读取hive表中的数据
		 */
		DataFrame table = hiveContext.table("psn9");
		table.show();
		
		/**
		 * 将数据保存到hive表中,
		 */
//		hiveContext.sql("create table if not exists spark_on_hive(name string, age int)");
//		sqlDataFrame.write().mode(SaveMode.Append).saveAsTable("spark_on_hive");
		
		jsc.stop();
		
	}
}
