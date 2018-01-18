package com.spark.study;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class Top3 {
	
	public static void main(String[] args) {
		
		
		
		SparkConf conf = new SparkConf()
				.setAppName("Top3")
				.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile("");
		
		JavaRDD<String> words = lines.flatMap(f -> Arrays.asList(f.split(" ")).iterator());
		
		JavaPairRDD<Integer,String> nums = words.mapToPair(f -> new Tuple2(Integer.valueOf(f),f));
		
		List<Tuple2<Integer, String>> res = nums.sortByKey().take(3);
		
		for(Tuple2 tuple:res){
			System.out.println(tuple._1);
		}

	}

}
