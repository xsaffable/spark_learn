package com.gjxx.java.spark.learn.secondary_sort;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;

import java.util.Comparator;
import java.util.List;

/**
 * @ClassName SecondarySort
 * @Description spark 二次排序
 * @Author SXS
 * @Date 2019/8/8 14:24
 * @Version 1.0
 */
public class SecondarySort {

    public static void main(String[] args) {
        // 连接spark master
        JavaSparkContext jsc = new JavaSparkContext("local", "secondarySort");

        // 读取文件,创建JavaRDD<String>
        JavaRDD<String> lines = jsc.textFile("file/daily_temperature");

        // 转化为键值对
        JavaPairRDD<Tuple2<String, Integer>, String> pairs = lines.mapToPair(SecondarySort::toPair);

        // 边分区边排序
        JavaPairRDD<Tuple2<String, Integer>, String> groups = pairs.repartitionAndSortWithinPartitions(new MyPartitioner(5), new MyComparator());

        // key: yearMonth
        // val: temperature
        JavaPairRDD<String, String> pairs2 = groups.mapToPair(SecondarySort::toPair2);

        // 把temperature加起来
        JavaPairRDD<String, String> result = pairs2.reduceByKey(SecondarySort::reduce, 1);

        List<Tuple2<String, String>> output = result.collect();
        for (Tuple2<String, String> t2 : output) {
            System.out.println(t2);
        }

    }

    /**
     * // 转化未键值对
     * @param line 文本中的每一行数据
     * @return Tuple2<Tuple2<String, Integer>, String>
     */
    private static Tuple2<Tuple2<String, Integer>, String> toPair(String line) {
        String[] fields = line.split(",");
        String yearMonth = fields[0].trim() + "-" + fields[1].trim();
        String day = fields[2].trim();
        int temperature = Integer.parseInt(fields[3].trim());
        return new Tuple2<>(new Tuple2<>(yearMonth, temperature), day);
    }

    /**
     *
     * @param t2 ((yearMonth, temperature), day)
     * @return (yearMonth, temperature)
     */
    private static Tuple2<String, String> toPair2(Tuple2<Tuple2<String, Integer>, String> t2) {
        return new Tuple2<>(t2._1._1, t2._1._2.toString());
    }

    /**
     * 聚合
     * @param v1 val1
     * @param v2 val2
     * @return val1, val2
     */
    private static String reduce(String v1, String v2) {
        return v1 + "," + v2;
    }


    /**
     * 自定义分区
     * 用于把元组key中yearMonth相同的数据分到同一组中
     */
    static class MyPartitioner extends Partitioner implements Serializable {

        private int numPartitions;

        MyPartitioner(int numPartitions) {
            this.numPartitions = numPartitions;
        }

        @Override
        public int numPartitions() {
            return this.numPartitions;
        }

        @Override
        public int getPartition(Object key) {
            // 把yearMonth相同的数据分到同一组中
            if (key instanceof Tuple2) {
                Tuple2 t2 = (Tuple2) key;
                return Math.abs(t2._1.hashCode() % this.numPartitions);
            } else {
                return Math.abs(key.hashCode() % this.numPartitions);
            }
        }
    }

    /**
     * 自定义比较器
     * 用于对元组key进行二次排序
     */
    static class MyComparator implements Comparator<Tuple2<String, Integer>>, Serializable {

        @Override
        public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
            // 先比较yearMonth
            int compareValue = o1._1.compareTo(o2._1);

            // 如果yearMonth相等,再比较temperature
            if (compareValue == 0) {
                compareValue = o1._2.compareTo(o2._2);
            }
            return compareValue;
        }
    }



}
