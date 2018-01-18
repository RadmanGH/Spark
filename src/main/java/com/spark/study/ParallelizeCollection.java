package com.spark.study;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ParallelizeCollection {
	
	public static void main(String[] args) {
		
		
		SparkConf conf = new SparkConf()
				.setAppName("ParallelizeCollection")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		
		JavaRDD<Integer>numberRDD = sc.parallelize(numbers);
		
		Integer sum = numberRDD.reduce((i1,i2) -> i1+i2);
		
		System.out.println(sum);
		
		sc.close();
		
	}

}
