package com.spark.study;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SortWordCount {
	
	 private static final Pattern SPACE = Pattern.compile(" ");
	 
	 public static void main(String[] args) {
		
	
	
		 SparkConf conf = new SparkConf()
	     		.setAppName("SortWordCount")
	     		.setMaster("local");
	
	     
	     JavaSparkContext sc = new JavaSparkContext(conf);
	     
	     JavaRDD<String> lines = sc.textFile("C://Users//superstar//Desktop//1.txt");
	     
	     JavaRDD<String> words = lines.flatMap(f -> Arrays.asList(SPACE.split(f)).iterator());
	
	     
	     JavaPairRDD<String,Integer> ones = words.mapToPair(f -> new Tuple2(f,1));
	     
	     JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1,i2) -> i1+i2);
	     
	     JavaPairRDD<Integer, String> sortcount1 = counts.mapToPair(f -> new Tuple2(f._2,f._1));
	     
	     JavaPairRDD<Integer, String> sortcount2 = sortcount1.sortByKey(false);
	     
	     JavaPairRDD<String, Integer> sortcount3 = sortcount2.mapToPair(f -> new Tuple2(f._2,f._1));
	     
	     List<Tuple2<String,Integer>> output= sortcount3.collect();
	     
	     for(Tuple2<String,Integer> tuple:output){
	     	System.out.println(tuple._1+": "+tuple._2);
	     }
	     
	     sc.stop();
	 } 
}
