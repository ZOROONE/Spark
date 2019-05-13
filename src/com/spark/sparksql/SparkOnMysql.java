package com.spark.sparksql;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SQLContext;



/**
 * 
 * 共2种连接，和其他连接其实差不多
 * @author ZORO
 *
 */
public class SparkOnMysql {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("mysql");
		
		JavaSparkContext jsc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(jsc);
		
		
		/**
		 * 第一种
		 */
		Properties properties = new Properties();
		properties.setProperty("driver", "com.mysql.jdbc.Driver");
		properties.setProperty("user", "root");
		properties.setProperty("password", "123");
		//因为这里已经指定了，url, 和dbtable, 所以properties就不用指定了
		DataFrame jdbcDataFrame = sqlContext.read().jdbc("jdbc:mysql://node001:3306/test", "hbase_to_mysql", properties);
		jdbcDataFrame.show();
		
		/**
		 * 第二种， 
		 */
		Map<String, String> options = new HashMap<String, String>();
		options.put("driver", "com.mysql.jdbc.Driver");
		options.put("url", "jdbc:mysql://node001:3306/test");
		options.put("user", "root");
		options.put("password", "123");
		options.put("dbtable", "hbase_to_mysql");
		
		DataFrame jdbcDataFrame2 = sqlContext.read().format("jdbc").options(options).load();
		jdbcDataFrame2.show();
		
		/**
		 * 第3种， 第二种的分开写
		 */
		DataFrameReader format = sqlContext.read().format("jdbc");
		format.option("driver", "com.mysql.jdbc.Driver");
		format.option("url", "jdbc:mysql://node001:3306/test");
		format.option("user", "root");
		format.option("password", "123");
		format.option("dbtable", "hbase_to_mysql");
		DataFrame jdbcDataFrame3 = format.load();
		jdbcDataFrame3.show();
		
		
		
		jsc.stop();
		
		
	}
}
