package com.gjxx.java.spark.learn;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkLearn {

    public static void main(String[] args) {

        List<String> list = new ArrayList<String>();
        list.add("123");
        list.add("22");
        list.add("hello");
        list.add("spark");

        JavaSparkContext sc = new JavaSparkContext("local[4]", "SparkLearn");

        JavaRDD<String> rdd = sc.textFile("E:/test.txt", 4);

//        JavaRDD<String> rdd2 = rdd.flatMap(new FlatMapFunction<String, String>() {
//            public Iterator<String> call(String line) throws Exception {
//                return Arrays.asList(line.split(" ")).iterator();
//            }
//        });
//
//        JavaPairRDD<String, Integer> rdd3 = rdd2.mapToPair(new PairFunction<String, String, Integer>() {
//            public Tuple2<String, Integer> call(String word) throws Exception {
//                return new Tuple2<String, Integer>(word, 1);
//            }
//        });
//
//        JavaPairRDD<String, Integer> rdd4 = rdd3.reduceByKey(new Function2<Integer, Integer, Integer>() {
//            public Integer call(Integer v1, Integer v2) throws Exception {
//                return v1 + v2;
//            }
//        });
//
//        rdd4.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                System.out.println("stringIntegerTuple2 = [" + stringIntegerTuple2 + "]");
//            }
//        });

        JavaPairRDD<String, Integer> rdd2 = rdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((v1, v2) -> (v1 + v2));

        rdd2.map(rs -> new Tuple2(rs._2, rs._1))
                .sortBy(rs -> rs._1, true, 1)
                .foreach(line -> System.out.println("line = [" + line + "]"));

    }

}
