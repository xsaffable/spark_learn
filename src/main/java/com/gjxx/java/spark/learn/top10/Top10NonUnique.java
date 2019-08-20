package com.gjxx.java.spark.learn.top10;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

/**
 * @ClassName Top10NonUnique
 * @Description 非唯一键的Top10
 * @Author SXS
 * @Date 2019/8/9 10:57
 * @Version 1.0
 */
public class Top10NonUnique {

    public static void main(String[] args) {
        // 连接spark master
        JavaSparkContext jsc = new JavaSparkContext("local", "top10");

        // 读取文件,创建JavaRDD<String>
        JavaRDD<String> lines = jsc.textFile("file/top10/*");

        // 返回一个新的rdd,归约到9个分区
        JavaRDD<String> rdd = lines.coalesce(9);

        // 映射成kv对
        JavaPairRDD<String, Integer> kvs = rdd.mapToPair(Top10NonUnique::toPair);

        // 归约重复键
        JavaPairRDD<String, Integer> uniqueKeys = kvs.reduceByKey(Integer::sum);

        // 创建本地(每个分区)top10
        JavaRDD<SortedMap<Integer, String>> partitions = uniqueKeys.mapPartitions(Top10NonUnique::mapTop10);

        // 查找最终的top10
        SortedMap<Integer, String> finalTop10 = new TreeMap<>();
        List<SortedMap<Integer, String>> allTop10 = partitions.collect();
        allTop10.forEach(sm -> sm.forEach((k, v) -> {
            finalTop10.put(k, v);
            if (finalTop10.size() > 10) {
                finalTop10.remove(finalTop10.firstKey());
            }
        }));
        finalTop10.forEach((k, v) -> System.out.println(k + "->" + v));

    }

    /**
     * 映射成kv对
     * @param s 输入的一行文本
     * @return kv对
     */
    private static Tuple2<String, Integer> toPair(String s) {
        String[] tokens = s.split(",");
        return new Tuple2<>(tokens[0], Integer.parseInt(tokens[1]));
    }

    /**
     * 创建本地(每个分区)top10
     * @param iter 每个分区的迭代器
     * @return 每个分区的top10
     */
    private static Iterator<SortedMap<Integer, String>> mapTop10(Iterator<Tuple2<String, Integer>> iter) {
        // 此处相当于MapReduce中Map的setup
        SortedMap<Integer, String> localTop10 = new TreeMap<>();

        // 此处相当于MapReduce中Map的map
        while (iter.hasNext()) {
            Tuple2<String, Integer> t2 = iter.next();
            localTop10.put(t2._2, t2._1);
            // 如果满了,则移除最小的kv
            if (localTop10.size() > 10) {
                localTop10.remove(localTop10.firstKey());
            }
        }

        // 此处相当于MapReduce中Map的cleanup方法
        return Collections.singleton(localTop10).iterator();
    }
}
