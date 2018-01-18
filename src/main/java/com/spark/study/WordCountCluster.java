package com.spark.study;


import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.math3.geometry.Space;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class WordCountCluster 
{
	private static final Pattern SPACE = Pattern.compile(" ");
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        SparkConf conf = new SparkConf()
        		.setAppName("WordCount");
//        		.setMaster("spark://VM1:7077");
        //master 设置为local时，为本地运行
        
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> lines = sc.textFile("hdfs://VM1:9000/tmp/1.txt");
        
        JavaRDD<String> words = lines.flatMap(f -> Arrays.asList(SPACE.split(f)).iterator());

        
        JavaPairRDD<String,Integer> ones = words.mapToPair(f -> new Tuple2(f,1));
        
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1,i2) -> i1+i2);
        
        List<Tuple2<String,Integer>> output= counts.collect();
        
        for(Tuple2<String,Integer> tuple:output){
        	System.out.println(tuple._1+": "+tuple._2);
        }
        sc.stop();
    }
}
