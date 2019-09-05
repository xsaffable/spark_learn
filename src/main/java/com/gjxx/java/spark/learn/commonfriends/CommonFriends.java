package com.gjxx.java.spark.learn.commonfriends;

import java.util.Set;
import java.util.TreeSet;

/**
 * @Description 查找共同好友的POJO解决方案
 * @Author Sxs
 * @Date 2019/9/5 15:40
 * @Version 1.0
 */
public class CommonFriends {

    // user1Friends = {A1, A2, ..., Am}
    // user2Friends = {B1, B2, ..., Bm}

    public static Set<Integer> intersection(Set<Integer> user1Friends, Set<Integer> user2Friends) {
        if ((user1Friends == null) || (user1Friends.isEmpty())) {
            return null;
        }
        if ((user2Friends == null) || (user2Friends.isEmpty())) {
            return null;
        }
        // 两个集合都非空
        if (user1Friends.size() < user2Friends.size()) {
            return intersect(user1Friends, user2Friends);
        } else {
            return intersect(user2Friends, user1Friends);
        }
    }

    private static Set<Integer> intersect(Set<Integer> smallSet, Set<Integer> largeSet) {
        Set<Integer> result = new TreeSet<>();
        // 迭代处理小集合来提高性能
        for (Integer x : smallSet) {
            if (largeSet.contains(x)) {
                result.add(x);
            }
        }
        return result;
    }

}
