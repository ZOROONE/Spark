package com.spark.sparksql;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DataFrameFromRDDSchema {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("RDDSchema");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(jsc);
		
		JavaRDD<String> lineRDD = jsc.parallelize(Arrays.asList("0 zhangsan 18", 
				"1 lisi 19", "1 wangwu 20"));
		
		/**
		 * 转换成Row类型的RDD，别导错包import org.apache.spark.sql.Row;
		 */
		JavaRDD<Row> rowRDD = lineRDD.map(new Function<String, Row>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(String v1) throws Exception {
				
				String[] fields = v1.split(" ");
				return RowFactory.create(fields[0], fields[1], Integer.valueOf(fields[2]));
			}
		});
		
		/**
		 *  动态构建DataFrame中的元数据，一般来说这里的字段可以来源自字符串，也可以来源于外部数据库
		 */
		
		List<StructField> fields = Arrays.asList(
				DataTypes.createStructField("id", DataTypes.StringType, true),
				DataTypes.createStructField("name", DataTypes.StringType, true),
				DataTypes.createStructField("age", DataTypes.IntegerType, true)
		);
		
		StructType schema = DataTypes.createStructType(fields);
		
		DataFrame personDataFrame = sqlContext.createDataFrame(rowRDD, schema);
		
		personDataFrame.write().mode(SaveMode.Overwrite).format("parquet").save("./data/parquet");
		personDataFrame.write().mode(SaveMode.Overwrite).parquet("./data/parquet");
		
		
		personDataFrame.registerTempTable("person");
		DataFrame result = sqlContext.sql("select name, age from person where age > 18");
		result.show(5);
	
		jsc.stop();
	}
}
