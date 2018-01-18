package com.spark.study;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class BroadcastVariable {
	
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("BroadcastVariable")
				.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		int factor = 3;
		
		Broadcast<Integer> factorBroadcast = sc.broadcast(factor);
		
		List nums = Arrays.asList(1,2,3,4,5,6);
		
		JavaRDD<Integer> rdd = sc.parallelize(nums);
		
		JavaRDD<Integer> res1 = rdd.map(f -> f*factor);
		
		JavaRDD<Integer> res2 = rdd.map(f -> f*factorBroadcast.value());
		
		res1.foreach(f ->{
			System.out.println("常数："+f);
		});
		
		res2.foreach(f ->{
			System.out.println("广播变量："+f);
		});
		
		
		
	}
}
