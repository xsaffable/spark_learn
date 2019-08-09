package com.gjxx.java.spark.learn;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

/**
 * @ClassName Top10
 * @Description 求Top10
 * @Author SXS
 * @Date 2019/8/8 17:55
 * @Version 1.0
 */
public class Top10 {

    public static void main(String[] args) {
        // 连接spark master
        JavaSparkContext jsc = new JavaSparkContext("local", "top10");

        // 读取文件,创建JavaRDD<String>
        JavaRDD<String> lines = jsc.textFile("file/ham.txt");

        // 把单词切分出来
        JavaRDD<String> words = lines.flatMap(Top10::toWords);

        // key: word
        // val: 1
        JavaPairRDD<String, Integer> kvs = words.mapToPair(Top10::toPair);

        // 聚合
        JavaPairRDD<String, Integer> result = kvs.reduceByKey(Integer::sum);

        // 为各个输入分区创建一个本地top10列表
        JavaRDD<SortedMap<Integer, String>> partitions = result.mapPartitions(Top10::mapTop10);

        // 收集所有本地top10并创建最终的top10列表
        SortedMap<Integer, String> result2 = partitions.reduce(Top10::reduceTop10);

        result2.forEach((k, v) -> System.out.println(k + "" + v));

    }

    /**
     * 把单词切分出来
     * @param s 文本的每一行数据
     * @return 每个单词
     */
    private static Iterator<String> toWords(String s) {
        String[] words = s.trim().replaceAll("[\\,|\\.|\\!|\\-]", "").split("\\s+");
        List<String> list = new ArrayList<>();
        Collections.addAll(list, words);
        return list.iterator();
    }

    /**
     * 在单词后面加上词频 1
     * @param word 文本中切分出来的单词
     * @return (单词, 1)
     */
    private static Tuple2<String, Integer> toPair(String word) {
        return new Tuple2<>(word, 1);
    }

    private static Iterator<SortedMap<Integer, String>> mapTop10(Iterator<Tuple2<String, Integer>> iter) {
        // 此处相当于MapReduce中Map的setup
        SortedMap<Integer, String> top10 = new TreeMap<>();

        // 此处相当于MapReduce中Map的map方法
        while (iter.hasNext()) {
            Tuple2<String, Integer> t2 = iter.next();
            top10.put(t2._2, t2._1);
            // 如果满了,则移除最小的kv
            if (top10.size() > 10) {
                top10.remove(top10.firstKey());
            }
        }

        // 此处相当于MapReduce中Map的cleanup方法
        return Collections.singleton(top10).iterator();
    }

    /**
     * 收集所有本地top10并创建最终的top10列表
     * @param m1 每个分区的top
     * @param m2 每个分区的top
     * @return 最终的top10列表
     */
    private static SortedMap<Integer, String> reduceTop10(SortedMap<Integer, String> m1, SortedMap<Integer, String> m2) {
        SortedMap<Integer, String> top10 = new TreeMap<>();
        m1.forEach((k, v) -> {
            top10.put(k, v);
            if (top10.size() > 10) {
                top10.remove(top10.firstKey());
            }
        });
        m2.forEach((k, v) -> {
            top10.put(k, v);
            if (top10.size() > 10) {
                top10.remove(top10.firstKey());
            }
        });
        return top10;
    }
}
