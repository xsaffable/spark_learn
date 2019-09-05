package com.gjxx.java.spark.learn.mba;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Description mba数据挖掘，计算支持度与置信度
 * @Author Sxs
 * @Date 2019/8/27 17:59
 * @Version 1.0
 */
public class FindAssociationRules {

    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext("local", "findAssociationRules");
        JavaRDD<String> transactions = jsc.textFile("file/mba/file.txt");
        // 生成频繁模式
        JavaPairRDD<List<String>, Integer> patterns = transactions.flatMapToPair(tranToPattern);
        // 规约频繁模式
        JavaPairRDD<List<String>, Integer> combined = patterns.reduceByKey(Integer::sum);
        // 生成所有子模式
        JavaPairRDD<List<String>, Tuple2<List<String>, Integer>> subPatterns = combined.flatMapToPair(FindAssociationRules.subPatterns);
        // 组合子模式
        JavaPairRDD<List<String>, Iterable<Tuple2<List<String>, Integer>>> rules = subPatterns.groupByKey();
        // 生成关联规则
        JavaRDD<List<Tuple3<List<String>, List<String>, Double>>> assocRules = rules.map(rulesToAssocRules);

        assocRules.collect().forEach(System.out::println);

        jsc.stop();
    }

    /**
     * 生成关联规则
     */
    private static Function<Tuple2<List<String>, Iterable<Tuple2<List<String>, Integer>>>, List<Tuple3<List<String>, List<String>, Double>>> rulesToAssocRules = rule -> {
        List<Tuple3<List<String>, List<String>, Double>> result = new ArrayList<>();
        List<String> fromList = rule._1;
        Iterable<Tuple2<List<String>, Integer>> to = rule._2;
        List<Tuple2<List<String>, Integer>> toList = new ArrayList<>();
        Tuple2<List<String>, Integer> fromCount = null;
        for (Tuple2<List<String>, Integer> t2 : to) {
            // 找到"count"对象
            if (t2._1 == null) {
                fromCount = t2;
            } else {
                toList.add(t2);
            }
        }
        // 得到生成关联规则所需的对象
        // fromList, fromCount, toList
        if (toList.isEmpty()) {
            // 没有生成输出，不过由于Spark不接受null对象，我们将模拟一个null对象
            // 返回一个空列表
            return result;
        }
        // 使用三个对象创建关联规则
        for (Tuple2<List<String>, Integer> t2 : toList) {
            assert fromCount != null;
            double confidence = (double) t2._2 / (double) fromCount._2;
            List<String> t2List = new ArrayList<>(t2._1);
            t2List.removeAll(fromList);
            result.add(new Tuple3<>(fromList, t2List, confidence));
        }
        return result;
    };

    /**
     * 生成所有子模式
     */
    private static PairFlatMapFunction<Tuple2<List<String>, Integer>, List<String>, Tuple2<List<String>, Integer>> subPatterns = pattern -> {
        List<Tuple2<List<String>, Tuple2<List<String>, Integer>>> result = new ArrayList<>();
        List<String> list = pattern._1;
        Integer frequency = pattern._2;
        result.add(new Tuple2<>(list, new Tuple2<>(null, frequency)));
        if (list.size() == 1) {
            return result.iterator();
        }

        // 模式中包含多个商品
        for (int i = 0; i < list.size(); i++) {
            List<String> subList = removeOneItem(list, i);
            result.add(new Tuple2<>(subList, new Tuple2<>(list, frequency)));
        }

        return result.iterator();
    };

    /**
     * 生成频繁模式
     */
    private static PairFlatMapFunction<String, List<String>, Integer> tranToPattern = transaction -> {
        // 把一次交易切分为每个商品
        List<String> list = toList(transaction);
        // 生成生成有序集的所有组合
        List<List<String>> combinations = Combination.findSortedCombinations(list);
        List<Tuple2<List<String>, Integer>> result = new ArrayList<>();
        for (List<String> combination : combinations) {
            if (combination.size() > 0) {
                result.add(new Tuple2<>(combination, 1));
            }
        }
        return result.iterator();
    };

    /**
     * 把一项交易转化为商品列表
     *
     * @param transaction 交易
     * @return 商品列表
     */
    private static List<String> toList(String transaction) {
        String[] items = transaction.trim().split(",");
        return new ArrayList<>(Arrays.asList(items));
    }

    /**
     * 从给定的列表中删除一项，返回删除了这一项的新列表
     *
     * @param list 原列表
     * @param i    要删除的那一项的下标
     * @return 删除项之后的新列表
     */
    private static List<String> removeOneItem(List<String> list, int i) {
        if ((list == null) || (list.isEmpty())) {
            return list;
        }
        if ((i < 0) || (i > (list.size() - 1))) {
            return list;
        }
        List<String> cloned = new ArrayList<>(list);
        cloned.remove(i);
        return cloned;
    }

}
