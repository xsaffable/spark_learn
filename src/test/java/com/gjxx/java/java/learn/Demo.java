package com.gjxx.java.java.learn;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

/**
 * @ClassName Demo
 * @Description TODO
 * @Author SXS
 * @Date 2019/8/14 9:55
 * @Version 1.0
 */
public class Demo {

    public static void main(String[] args) {
        Set<String> set = new TreeSet<>();
        set.add("1");
        set.add("2");
        set.add("5");
        set.add("3");

        Object[] objects = set.toArray();
        String[] ss = new String[objects.length];
        for (int i = 0; i < objects.length; i++) {
            ss[i] = (String) objects[i];
        }
        System.out.println(Arrays.toString(ss));

    }

}
