package com.spark.study;


import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.math3.geometry.Space;
import org.apache.hadoop.hive.metastore.api.Function;
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
public class WordCountLocal 
{
	private static final Pattern SPACE = Pattern.compile(" ");
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        SparkConf conf = new SparkConf()
        		.setAppName("WordCount")
        		.setMaster("local1");
        //master 设置为local时，为本地运行
//        SparkSession spark = SparkSession.builder()
//        		.appName("WordCount")
//        		.getOrCreate();
        
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> lines = sc.textFile("C://Users//superstar//Desktop//1.txt");
        
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String,String>() {
        	
        	private static final long serialVersionUID=1L;

			@Override
			public Iterator<String> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(arg0.split(" ")).iterator();
			}
		});
        
        JavaRDD<String> words1 = lines.flatMap(f -> Arrays.asList(f.split(" ")).iterator());

        
        JavaPairRDD<String,Integer> ones = words.mapToPair(f -> new Tuple2(f,1));
        
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1,i2) -> i1+i2);
        
        List<Tuple2<String,Integer>> output= counts.collect();
        
        for(Tuple2<String,Integer> tuple:output){
        	System.out.println(tuple._1+": "+tuple._2);
        }
        sc.stop();
    }
}
