package com.gjxx.java.spark.learn.commonfriends;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.*;

/**
 * @Description 查找共同好友
 * @Author Sxs
 * @Date 2019/9/5 16:27
 * @Version 1.0
 */
public class FindCommonFriends {

    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext("local", "findAssociationRules");
        JavaRDD<String> records = jsc.textFile("file/commonfriends/file.txt");

        // 将JavaRDD<String>映射为(key, value)对
        // 其中key=Tuple<user1, user2>, value=好友列表
        JavaPairRDD<Tuple2<Long, Long>, List<Long>> pairs = records.flatMapToPair(strToPairs);

//        // 应用规约器
//        JavaPairRDD<Tuple2<Long, Long>, Iterable<List<Long>>> grouped = pairs.groupByKey();
//
//        // 查找所有的共同好友
//        JavaPairRDD<Tuple2<Long, Long>, List<Long>> commonFriends = grouped.mapValues(groupedToCommon);

        // 合并以上两个步骤
        JavaPairRDD<Tuple2<Long, Long>, List<Long>> commonFriends = pairs.reduceByKey(pairsToCommon);

        commonFriends.collect().forEach(System.out::println);

        jsc.stop();
    }

    private static Function2<List<Long>, List<Long>, List<Long>> pairsToCommon = (list1, list2) -> {
        List<Long> result = new ArrayList<>();
        list1.forEach(l -> {
            if (list2.contains(l)) {
                result.add(l);
            }
        });
        return result;
    };

    private static Function<Iterable<List<Long>>, List<Long>> groupedToCommon = iter -> {
        Map<Long, Integer> countCommon = new HashMap<>();
        int size = 0;
        for (List<Long> list : iter) {
            size++;
            if ((list == null) || (list.isEmpty())) {
                continue;
            }
            for (Long l : list) {
                Integer count = countCommon.get(l);
                if (count == null) {
                    countCommon.put(l, 1);
                } else {
                    countCommon.put(l, ++count);
                }
            }
        }

        // 如果value==size，那么就有一个共同好友
        List<Long> finalCommonFriends = new ArrayList<>();
        int finalSize = size;
        countCommon.forEach((k, v) -> {
            if (v == finalSize) {
                finalCommonFriends.add(k);
            }
        });
        return finalCommonFriends;
    };

    /**
     * 将JavaRDD<String>映射为(key, value)对
     */
    private static PairFlatMapFunction<String, Tuple2<Long, Long>, List<Long>> strToPairs = str -> {
        String[] tokens = str.split(",");
        long person = Long.parseLong(tokens[0]);
        String friendsAsString = tokens[1].trim();
        String[] friendsTokenized = friendsAsString.split(" ");
        if (friendsTokenized.length == 1) {
            Tuple2<Long, Long> key = buildSortedTuple(person, Long.parseLong(friendsTokenized[0]));
            return Collections.singletonList(new Tuple2<Tuple2<Long, Long>, List<Long>>(key, new ArrayList<>())).iterator();
        }
        List<Long> friends = new ArrayList<>();
        for (String f : friendsTokenized) {
            friends.add(Long.parseLong(f));
        }
        List<Tuple2<Tuple2<Long, Long>, List<Long>>> result = new ArrayList<>();
        for (Long f : friends) {
            Tuple2<Long, Long> key = buildSortedTuple(person, f);
            result.add(new Tuple2<>(key, friends));
        }
        return result.iterator();
    };

    /**
     * 对a, b排序，确保不会得到重复的Tuple<user1, user2>对象
     * @param a user1
     * @param b user2
     * @return Tuple(user1, user2) or Tuple(user2, user1)
     */
    private static Tuple2<Long, Long> buildSortedTuple(long a, long b) {
        if (a <= b) {
            return new Tuple2<>(a, b);
        } else {
            return new Tuple2<>(b, a);
        }
    }

}
