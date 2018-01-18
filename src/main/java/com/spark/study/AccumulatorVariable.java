package com.spark.study;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AccumulatorVariable {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
				.setAppName("Accumulator")
				.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);

		List nums = Arrays.asList(1,2,3,4,5,6);
		
		JavaRDD<Integer> rdd = sc.parallelize(nums);
		
		Accumulator<Integer> sum = sc.accumulator(0);
		
		rdd.foreach(f -> sum.add(f));
		
		System.out.println(sum.value());
	}

}
