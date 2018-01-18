package com.spark.study;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class CountLocalFile {

	public static void main(String[] args) {
		
		
		SparkConf conf = new SparkConf()
				.setAppName("CountLocalFile")
				.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String>linesRDD = sc.textFile("C://Users//superstar//Desktop//1.txt");
		
		JavaRDD<Integer> lengths = linesRDD.map(line -> line.length());
		
		int count = lengths.reduce((i1,i2) -> i1+i2);
		
		System.out.println(count);
		
		sc.close();
	}
}
