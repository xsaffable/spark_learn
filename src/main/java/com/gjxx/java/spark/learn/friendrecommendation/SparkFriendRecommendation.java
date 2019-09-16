package com.gjxx.java.spark.learn.friendrecommendation;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.*;

/**
 * @Description 推荐可能的好友
 * @Author Sxs
 * @Date 2019/9/10 14:50
 * @Version 1.0
 */
public class SparkFriendRecommendation {

    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext("local", "sparkFriendRecommendation");
        JavaRDD<String> records = jsc.textFile("file/friendrecommendation/friendS2.txt", 1);

        // k: person
        // v: 直接好友关系和将来可能的好友关系
        JavaPairRDD<Long, Tuple2<Long, Long>> pairs = records.flatMapToPair(str2Pair);

//        // 分组
//        JavaPairRDD<Long, Iterable<Tuple2<Long, Long>>> grouped = pairs.groupByKey();
//
//        // 生成最后推荐的好友
//        JavaPairRDD<Long, String> recommendations = grouped.mapValues(group2rec);

        // 可以替换为combineByKey
        JavaPairRDD<Long, Map<Long, List<Long>>> recommendations = pairs.combineByKey(createCombiner, mergeValue, mergeCombiners);

        recommendations.collect().forEach(System.out::println);

        jsc.stop();
    }

    /**
     * 创建combiner时调用
     * 一个人有多个推荐好友，
     * 每个推荐好友有多个对应的共同好友
     * {person: {toUser: [mutualFriend]}}
     */
    private static Function<Tuple2<Long, Long>, Map<Long, List<Long>>> createCombiner = t2 -> {
        Long toUser = t2._1;
        Long mutualFriend = t2._2;
        Map<Long, List<Long>> recommendFriends = new HashMap<>();
        // 如果是直接好友
        if (mutualFriend == -1) {
            recommendFriends.put(toUser, null);
        } else {
            List<Long> mutualFriends = new ArrayList<>();
            mutualFriends.add(mutualFriend);
            recommendFriends.put(toUser, mutualFriends);
        }

        return recommendFriends;
    };

    /**
     * 在分区内每次合并值时调用
     */
    private static Function2<Map<Long, List<Long>>, Tuple2<Long, Long>, Map<Long, List<Long>>> mergeValue = (m, t2) -> {
        Long toUser = t2._1;
        Long mutualFriend = t2._2;

        // 如果推荐好友中已经包括此toUser
        if (m.containsKey(toUser)) {
            // 如果是-1，证明两人是直接好友
            if (mutualFriend == -1) {
                m.put(toUser, null);
            } else if (m.get(toUser) != null) {
                m.get(toUser).add(mutualFriend);
            }
        } else {
            // 如果是-1，证明两人是直接好友
            if (mutualFriend == -1) {
                m.put(toUser, null);
            } else {
                List<Long> list = new ArrayList<>();
                list.add(mutualFriend);
                m.put(toUser, list);
            }
        }
        return m;
    };

    /**
     * 分区之间的combiner合并时调用
     */
    private static Function2<Map<Long, List<Long>>, Map<Long, List<Long>>, Map<Long, List<Long>>> mergeCombiners = (m1, m2) -> {
        m2.forEach((m2ToUser, m2MutualFriends) -> {
            // 如果m1中包含此toUser
            if (m1.containsKey(m2ToUser)) {
                List<Long> m1MutualFriends = m1.get(m2ToUser);
                if ((m1MutualFriends != null) && (m2MutualFriends != null)) {
                    m2MutualFriends.forEach(m2MutualFriend -> {
                        if (!m1MutualFriends.contains(m2MutualFriend)) {
                            m1MutualFriends.add(m2MutualFriend);
                        }
                    });
                } else {
                    m1.put(m2ToUser, null);
                }
            } else {
                // 不包含此toUser
                m1.put(m2ToUser, m2MutualFriends);
            }
        });
        return m1;

    };

    /**
     * 生成最后推荐的好友
     */
    private static Function<Iterable<Tuple2<Long, Long>>, String> group2rec = iter -> {

        final Map<Long, List<Long>> mutualFriends = new HashMap<>();
        iter.forEach(t2 -> {
            Long toUser = t2._1;
            Long mutualFriend = t2._2;
            // 如果是-1，则两人已经是好友
            boolean alreadyFriend = (mutualFriend == -1);
            // 如果mutualFriends所包含的k对应的value已经是null，则证明toUser是person的直接好友
            // 如果mutualFriend=-1，则直接放入(toUser, null)，证明是直接好友，以后不会再改变其值
            if (mutualFriends.containsKey(toUser)) {
                if (alreadyFriend) {
                    mutualFriends.put(toUser, null);
                } else if (mutualFriends.get(toUser) != null) {
                    mutualFriends.get(toUser).add(mutualFriend);
                }
            } else {
                if (alreadyFriend) {
                    mutualFriends.put(toUser, null);
                } else {
                    List<Long> list1 = new ArrayList<>(Collections.singletonList(mutualFriend));
                    mutualFriends.put(toUser, list1);
                }
            }
        });
        return buildRecommendations(mutualFriends);
    };

    private static PairFlatMapFunction<String, Long, Tuple2<Long, Long>> str2Pair = record -> {

        String[] tokens = record.split(" ");
        // 取出 "本人"
        long person = Long.parseLong(tokens[0]);
        String friendsAsString = tokens[1];
        String[] friendsTokenized = friendsAsString.split(",");

        // 取出 "本人" 和其直接的好友
        List<Long> friends = new ArrayList<>();
        List<Tuple2<Long, Tuple2<Long, Long>>> mapperOutput = new ArrayList<>();
        for (String friendAsString : friendsTokenized) {
            long toUser = Long.parseLong(friendAsString);
            friends.add(toUser);
            Tuple2<Long, Long> directFriend = t2(toUser, -1L);
            mapperOutput.add(t2(person, directFriend));
        }

        // 取出好友间可能的好友
        for (int i = 0; i < friends.size(); i++) {
            for (int j = i + 1; j < friends.size(); j++) {
                // 可能的好友1
                Tuple2<Long, Long> possibleFriend1 = t2(friends.get(j), person);
                mapperOutput.add(t2(friends.get(i), possibleFriend1));
                // 可能的好友2
                Tuple2<Long, Long> possibleFriend2 = t2(friends.get(i), person);
                mapperOutput.add(t2(friends.get(j), possibleFriend2));
            }
        }

        return mapperOutput.iterator();
    };

    /**
     * 建立推荐关系
     * @param mutualFriends 共同好友
     * @return String
     */
    private static String buildRecommendations(Map<Long, List<Long>> mutualFriends) {
        StringBuilder recommendations = new StringBuilder();
        mutualFriends.forEach((k, v) -> {
            if (v == null) {
                // 已经是好友，不需要再次推荐
                // forEach中使用return跳过本次循环，不能使用break跳出循环
                return;
            }
            recommendations.append(k)
                    .append(" (")
                    .append(v.size())
                    .append(":")
                    .append(v)
                    .append("),");
        });
        return recommendations.toString();
    }

    private static Tuple2<Long, Long> t2(long a, long b) {
        return new Tuple2<>(a, b);
    }

    private static Tuple2<Long, Tuple2<Long, Long>> t2(long a, Tuple2<Long, Long> b) {
        return new Tuple2<>(a, b);
    }

}
