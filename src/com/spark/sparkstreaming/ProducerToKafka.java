package com.spark.sparkstreaming;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

public class ProducerToKafka extends Thread{
	
	private static String[] channelNames = {"spark", "kafka", "hadoop", 
			"flume", "hive", "hbase", "zookeeper", "storm", "python", "redis", "es"};
	
	private static String[] actionNames = {"view", "register"};
	
	private static String today;
	private static String topic;
	private static Random random;
	private static Producer<Integer, String> producerForKafka;
	
	public ProducerToKafka(String topic) {
		this.topic = topic;
		this.today = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
		this.random = new Random();
		Properties properties = new Properties();
		properties.setProperty("metadata.broker.list", "node001:9092,node002:9092,node003:9092");
		properties.setProperty("serializer.class", StringEncoder.class.getName());
		this.producerForKafka = new Producer<Integer, String>(new ProducerConfig(properties));
		 
		
	}
	
	@Override
	public void run() {
		int counter = 0;
		while (true) {
			counter++;
			String userLog = createUserLog();
			
			producerForKafka.send(new KeyedMessage<Integer, String>(topic,userLog));
			if(0 == counter%2) {
				counter = 0;
				try {
					Thread.sleep(2000);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	private String createUserLog() {
		
		long timeStamp = new Date().getTime();
		Long userID = 0L;
		if(random.nextInt(8) == 0) {
			userID = null;
		} else {
			userID = (long) random.nextInt(2000);
		}
		
		long pageID = random.nextInt(2000);
		String channel = channelNames[random.nextInt(11)];
		String action = actionNames[random.nextInt(2)];
		
		StringBuffer buffer = new StringBuffer();
		
		buffer.append(today).append("\t")
			  .append(timeStamp).append("\t")
			  .append(userID).append("\t")
			  .append(pageID).append("\t")
			  .append(channel).append("\t")
			  .append(action).append("\t");
		
		return buffer.toString();
	}
	
	
	
	public static void main(String[] args) {
		new ProducerToKafka("userLog").start();
	}
	
}
