package com.gjxx.java.spark.learn.leftoutjoin;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @ClassName LeftOutJoinUsingLeftOuterJoin
 * @Description 使用spark的leftOuterJoin左外连接实现
 * @Author SXS
 * @Date 2019/8/15 17:50
 * @Version 1.0
 */
public class LeftOutJoinUsingLeftOuterJoin {

    public static void main(String[] args) {
        // 连接spark master
        JavaSparkContext jsc = new JavaSparkContext("local", "leftOuterJoin");

        // 读取文件,创建JavaRDD<String>
        JavaRDD<String> usersLines = jsc.textFile("file/leftouterjoins/users.txt");
        JavaRDD<String> transactionsLines = jsc.textFile("file/leftouterjoins/transactions.txt");

        // 为用户创建一个JavaRDD
        JavaPairRDD<String, String> usersRDD = usersLines.mapToPair(LeftOutJoinUsingLeftOuterJoin::usersToPair);

        // 为交易创建一个JavaRDD
        JavaPairRDD<String, String> transactionsRDD = transactionsLines.mapToPair(LeftOutJoinUsingLeftOuterJoin::transactionsToPair);

        // 左外连接
        JavaPairRDD<String, Tuple2<String, Optional<String>>> joined = transactionsRDD.leftOuterJoin(usersRDD);

        // k: product
        // v: location
        JavaPairRDD<String, String> products = joined.mapToPair(LeftOutJoinUsingLeftOuterJoin::toPair);

        // 去除每个product对应的location中重复值
        JavaPairRDD<String, Set<String>> productUniqueLocations = products.combineByKey(createCombiner, mergeValue, mergeCombiners);

        List<Tuple2<String, Set<String>>> result = productUniqueLocations.collect();

        result.forEach(System.out::println);

    }

    /**
     * @param s 输入的一行记录
     * @return key: userId
     *         val: locationalId
     */
    private static Tuple2<String, String> usersToPair(String s) {
        String[] userRecord = s.split(" ");
        return new Tuple2<>(userRecord[0], userRecord[1]);
    }

    /**
     * @param s 输入的一行记录
     * @return key: userId
     *         val: productId
     */
    private static Tuple2<String, String> transactionsToPair(String s) {
        String[] transactionRecord = s.split(" ");
        return new Tuple2<>(transactionRecord[2], transactionRecord[1]);
    }

    /**
     * k: product
     * v: location
     * @param t2 传入的元组
     * @return (k, v)
     */
    private static Tuple2<String, String> toPair(Tuple2<String, Tuple2<String, Optional<String>>> t2) {

        Tuple2<String, Optional<String>> value = t2._2;

        return new Tuple2<>(value._1, value._2.get());
    }

    /**
     * 创建combiner时调用
     */
    private static Function<String, Set<String>> createCombiner = (s -> {
        Set<String> set = new HashSet<>();
        set.add(s);
        return set;
    });

    /**
     * 在分区内每次合并值时调用
     */
    private static Function2<Set<String>, String, Set<String>> mergeValue = ((set, s) -> {
        set.add(s);
        return set;
    });

    /**
     * 分区之间的combiner合并时调用
     */
    private static Function2<Set<String>, Set<String>, Set<String>> mergeCombiners = ((set, set2) -> {
        set.addAll(set2);
        return set;
    });
}
