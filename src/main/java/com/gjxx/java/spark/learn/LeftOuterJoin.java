package com.gjxx.java.spark.learn;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

/**
 * @ClassName LeftOuterJoin
 * @Description spark左外连接实现
 * @Author SXS
 * @Date 2019/8/12 15:27
 * @Version 1.0
 */
public class LeftOuterJoin {

    public static void main(String[] args) {
        // 连接spark master
        JavaSparkContext jsc = new JavaSparkContext("local", "leftOuterJoin");

        // 读取文件,创建JavaRDD<String>
        JavaRDD<String> usersLines = jsc.textFile("file/leftouterjoins/users.txt");
        JavaRDD<String> transactionsLines = jsc.textFile("file/leftouterjoins/transactions.txt");

        // 为用户创建一个JavaRDD
        JavaPairRDD<String, Tuple2<String, String>> usersRDD = usersLines.mapToPair(LeftOuterJoin::usersToPair);

        // 为交易创建一个JavaRDD
        JavaPairRDD<String, Tuple2<String, String>> transactionsRDD = transactionsLines.mapToPair(LeftOuterJoin::transactionsToPair);

        // 合并RDD
        JavaPairRDD<String, Tuple2<String, String>> allRDD = transactionsRDD.union(usersRDD);

        // 根据userID对allRDD分组
        JavaPairRDD<String, Iterable<Tuple2<String, String>>> groupedRDD = allRDD.groupByKey();

        // 创建location -> products
        JavaPairRDD<String, String> productLocationsRDD = groupedRDD.flatMapToPair(LeftOuterJoin::toPLS);

        // 查找一个商品的所有地址
        JavaPairRDD<String, Iterable<String>> productByLocations = productLocationsRDD.groupByKey();

        // 对每个商品对应的地址进行去重
        JavaPairRDD<String, Tuple2<Set<String>, Integer>> productByUniqueLocations = productByLocations.mapValues(LeftOuterJoin::duplicateRemoval);

        List<Tuple2<String, Tuple2<Set<String>, Integer>>> result = productByUniqueLocations.collect();

        result.forEach(System.out::println);

    }


    /**
     * @param s 输入的一行记录
     * @return key: userId
     *         val: (L, locationalId)
     */
    private static Tuple2<String, Tuple2<String, String>> usersToPair(String s) {
        String[] userRecord = s.split(" ");
        Tuple2<String, String> location = new Tuple2<>("L", userRecord[1]);
        return new Tuple2<>(userRecord[0], location);
    }

    /**
     * @param s 输入的一行记录
     * @return key: userId
     *         val: (P, productId)
     */
    private static Tuple2<String, Tuple2<String, String>> transactionsToPair(String s) {
        String[] transactionRecord = s.split(" ");
        Tuple2<String, String> product = new Tuple2<>("P", transactionRecord[1]);
        return new Tuple2<>(transactionRecord[2], product);
    }


    /**
     * 每个分区调用一次本方法
     * @param iter
     * @return
     */
    private static Iterator<Tuple2<String, String>> toPLS(Tuple2<String, Iterable<Tuple2<String, String>>> iter) {
        // 此处相当于MapReduce中Map的setup
        // 一个userID下对应的所有value
        Iterable<Tuple2<String, String>> pairs = iter._2;
        String location = "UNKNOWN";
        List<String> products = new ArrayList<>();

        // 此处相当于MapReduce中Map的map
        for (Tuple2<String, String> t2 : pairs) {
            if ("L".equals(t2._1)) {
                location = t2._2;
            } else {
                products.add(t2._2);
            }
        }

        // 此处相当于MapReduce中Map的cleanup方法
        // 发出(k, v)对
        List<Tuple2<String, String>> kvs = new ArrayList<>();
        for (String product : products) {
            kvs.add(new Tuple2<>(product, location));
        }

        return kvs.iterator();

    }

    private static Tuple2<Set<String>, Integer> duplicateRemoval(Iterable<String> iter) {

        Set<String> uniqueLocations = new HashSet<>();
        for (String location : iter) {
            uniqueLocations.add(location);
        }

        return new Tuple2<>(uniqueLocations, uniqueLocations.size());

    }
}
