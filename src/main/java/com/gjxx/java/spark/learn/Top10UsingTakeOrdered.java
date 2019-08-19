package com.gjxx.java.spark.learn;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;

import java.util.Comparator;
import java.util.List;

/**
 * @ClassName Top10UsingTakeOrdered
 * @Description 使用takeOrdered获取top10
 * @Author SXS
 * @Date 2019/8/12 15:04
 * @Version 1.0
 */
public class Top10UsingTakeOrdered {

    public static void main(String[] args) {
        // 连接spark master
        JavaSparkContext jsc = new JavaSparkContext("local", "top10");

        // 读取文件,创建JavaRDD<String>
        JavaRDD<String> lines = jsc.textFile("file/top10/*");

        // 映射成kv对
        JavaPairRDD<String, Integer> kvs = lines.mapToPair(Top10UsingTakeOrdered::toPair);

        // 归约重复键
        JavaPairRDD<String, Integer> uniqueKeys = kvs.reduceByKey(Integer::sum);

        // 得到最终的top10
        List<Tuple2<String, Integer>> result = uniqueKeys.takeOrdered(3, MyTupleComparator.INSTANCE);

        result.forEach(System.out::println);

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

    static class MyTupleComparator implements Comparator<Tuple2<String, Integer>>, Serializable {

        final static MyTupleComparator INSTANCE = new MyTupleComparator();

        @Override
        public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
            return -o1._2.compareTo(o2._2);
        }
    }

}
