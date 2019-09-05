package com.gjxx.java.spark.learn.mba;

import java.util.*;

/**
 * @Description 生成有序集的所有组合
 * 例如：a, b, c -> [], [a], [b], [c], [a, b], [a, c], [b, c], [a, b, c]
 * @Author Sxs
 * @Date 2019/9/4 17:14
 * @Version 1.0
 */
public class Combination {

    public static void main(String[] args) {
        List<String> list = Arrays.asList("a", "b", "c");
        List<List<String>> comb = findSortedCombinations(list);
        System.out.println(comb.size());
        System.out.println(comb);
    }

    /**
     * 返回所有不同大小的组合
     * @param elements 所有元素
     * @param <T> 元素类型
     * @return 所有大小不同的组合
     */
    public static <T extends Comparable<? super T>> List<List<T>> findSortedCombinations(Collection<T> elements) {
        List<List<T>> result = new ArrayList<>();
        for (int i = 0; i <= elements.size(); i++) {
            result.addAll(findSortedCombinations(elements, i));
        }
        return result;
    }

    /**
     * elements=[a, b, c] -> findSortedCombinations(elements, 2): [[a, b], [a, c], [b, c]]
     * @param elements 所有元素
     * @param n 商品对的大小
     * @param <T> 元素类型
     * @return findSortedCombinations(elements, 2): [[a, b], [a, c], [b, c]]
     */
    private static <T extends Comparable<? super T>> List<List<T>> findSortedCombinations(Collection<T> elements, int n) {
        List<List<T>> result = new ArrayList<>();
        if (n == 0) {
            result.add(new ArrayList<>());
            return result;
        }
        List<List<T>> combinations = findSortedCombinations(elements, n - 1);
        for (List<T> combination : combinations) {
            for (T element : elements) {
                if (combination.contains(element)) {
                    continue;
                }

                List<T> list = new ArrayList<>(combination);
                if (list.contains(element)) {
                    continue;
                }

                list.add(element);
                Collections.sort(list);
                if (result.contains(list)) {
                    continue;
                }
                result.add(list);
            }
        }

        return result;
    }

}
