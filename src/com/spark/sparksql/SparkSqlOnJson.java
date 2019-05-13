package com.spark.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 该篇虽然是json，但是parquet， orc等文件类型同理：
 * 
 * 1. 读取json格式数据文件，本地文件，集群文件
 * 
 * 2. 存储为json个数数据文件，本地或者集群
 * 
 * 3. DataFrame.show()  默认显示20行
 * 
 * 4. DataFrame.registerTempTable("person"); 
 * 	      注册临时表person，没有雾化到磁盘，列名显示顺序是按照ascii顺序显示列。
 * 
 * 5. sqlContext.sql("select name , age from person");在里面写sql语句
 * 
 * 6. 查看DataFrame的schema信息
 * 
 * @author ZORO
 *
 */
public class SparkSqlOnJson {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf();
		conf.setMaster("local").setAppName("sparksqlonjson");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		SQLContext sqlContext = new SQLContext(jsc);
		
		/**
		 * 1. 读取json文件，
		 */
		//DataFrame jsonDataFrame = sqlContext.read().json("./data/SparkSql/json");
		DataFrame jsonDataFrame = sqlContext.read().format("json").load("hdfs://node001:8020/user/root/spark/sparksql/json.txt");
		
		
		/**
		 * 2. 存储为json格式的文件
		 */
		
		//jsonDataFrame.write().mode(SaveMode.Overwrite).json("hdfs://node001:8020/user/root/spark/sparksql/json.txt");
		//jsonDataFrame.write().mode(SaveMode.Overwrite).format("json").save("./data/SparkSql/json.txt");
		
		/**
		 * 3.默认显示20行， 可以自己指定
		 */
		jsonDataFrame.show();
		
		/**
		 * 4.注册内存中的临时表，没有雾化到磁盘
		 */
		jsonDataFrame.registerTempTable("person");
		
		/**
		 * 5. 查询数据，或者根据自己的业务需求写sql处理
		 */
		DataFrame nameAgeDataFrame = sqlContext.sql("select name, age from person");
		nameAgeDataFrame.show();
		
		/**
		 * 6. 打印DataFrame的schema的信息
		 * 结果：
		 * root
 		 *  |-- name: string (nullable = true)
         *  |-- age: long (nullable = true)
		 */
		nameAgeDataFrame.printSchema();
		jsc.stop();
	}
}
