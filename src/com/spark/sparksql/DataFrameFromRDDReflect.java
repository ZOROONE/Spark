package com.spark.sparksql;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * 通过反射的方式将非json格式的RDD转换成DataFrame
 * 注意：这种方式不推荐使用
 * @author ZORO
 */
public class DataFrameFromRDDReflect {
	
	public static void main(String[] args) {
		
		/**
		 * 注意：
		 * 1.自定义类要实现序列化接口
		 * 2.自定义类访问级别必须是Public
		 * 3.RDD转成DataFrame会把自定义类中字段的名称按assci码排序
		 */
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("reflect");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(jsc);
		
		JavaRDD<String> lineRDD = jsc.parallelize(Arrays.asList(
				"0 zhangsan 18", "1 lisi 19", "2 wangwu 20"));
		
		JavaRDD<Person> personRDD = lineRDD.map(new Function<String, Person>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Person call(String v1) throws Exception {
				String[] fields = v1.split(" ");
				Person person = new Person();
				person.setAge(Integer.valueOf(fields[2]));
				person.setName(fields[1]);
				person.setId(fields[0]);
				return person;
			}
		});
		
		/**
		 * 传入进去Person.class的时候，sqlContext是通过反射的方式创建DataFrame
		 * 在底层通过反射的方式获得Person的所有field，结合RDD本身，就生成了DataFrame
		 */
		DataFrame personDataFrame = sqlContext.createDataFrame(personRDD, Person.class);
		personDataFrame.registerTempTable("person");
		DataFrame result = sqlContext.sql("select name age from person where age > 18");
		result.show();
		
		
		/**
		 * 将DataFrame转成JavaRDD
		 * 注意：
		 * 1.可以使用row.getInt(0),row.getString(1)...
		 * 通过下标获取返回Row类型的数据，但是要注意列顺序问题---不常用
		 * 2.可以使用row.getAs("列名")来获取对应的列值。
		 * 
		 */
		JavaRDD<Row> rowRDD = personDataFrame.javaRDD();
		
		rowRDD.map(new Function<Row, Person>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Person call(Row v1) throws Exception {
				
				//按字节码排序
				Person person = new Person();
//				person.setId(v1.getString(1));
//				person.setName(v1.getString(2));
//				person.setAge(v1.getInt(0));
//				
				person.setId((String) v1.getAs("id"));
				person.setName((String) v1.getAs("name").toString());
				person.setAge((Integer)v1.getAs("age"));
				return person;
				
				
			}
		}).foreach(new VoidFunction<Person>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Person t) throws Exception {
				System.out.println(t);
			}
		});
		
		jsc.stop();
	}
}
