package com.gjxx.java.spark.learn.mllib;

import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description 朴素贝叶斯
 * @Author Sxs
 * @Date 2019/9/23 18:41
 * @Version 1.0
 */
public class BuildNaiveBayesClassifier implements Serializable {

    public static void main(String[] args) {

        // 步骤2: 处理输入参数
        if (args.length < 1) {
            System.err.println("Usage: BuildNaiveBayesClassifier <training-data-filename>");
            System.exit(1);
        }
        final String trainingDataFilename = args[0];

        // 步骤3: 创建一个Spark上下文对象
        JavaSparkContext jsc = new JavaSparkContext("local", "BuildNaiveBayesClassifier");

        // 步骤4: 读取训练数据集
        JavaRDD<String> training = jsc.textFile(trainingDataFilename);
        // 得到训练数据大小，这个大小将在计算条件概率时用到
        long trainingDataSize = training.count();

        // 步骤5: 对训练数据的所有元素实现map函数
        JavaPairRDD<Tuple2<String, String>, Integer> pairs = training.flatMapToPair(str2pair);

        // 步骤6: 对训练数据的所有元素实现reduce函数
        JavaPairRDD<Tuple2<String, String>, Integer> counts = pairs.reduceByKey(Integer::sum);

        // 步骤7: 收集归约数据为Map
        Map<Tuple2<String, String>, Integer> countsAsMap = counts.collectAsMap();

        // 步骤8: 建立分类器数据结构
        // 对新数据分类，我们需要建立以下结构：
        // 1. 概率表(PT)
        // 2. 分类列表(CLASSIFICATIONS)
        Tuple2<Map<Tuple2<String, String>, Double>, List<String>> classification = buildClassification(countsAsMap, trainingDataSize);

        // 步骤9: 保存分类器
        classification._1.forEach((k, v) -> System.out.println(k + "\t" + v));

        classification._2.forEach(System.out::println);

        jsc.stop();

    }

    /**
     * 建立分类器数据结构
     * @param countsAsMap 数据集
     * @param trainingDataSize 数据总数
     */
    private static Tuple2<Map<Tuple2<String, String>, Double>, List<String>> buildClassification(Map<Tuple2<String, String>, Integer> countsAsMap, long trainingDataSize) {
        Map<Tuple2<String, String>, Double> PT = new HashMap<>(16);
        List<String> CLASSIFICATIONS = new ArrayList<>();
        countsAsMap.forEach((k, v) -> {
            String classification = k._2;
            if ("CLASS".equals(k._1)) {
                PT.put(k, ((double) v / (double)trainingDataSize));
                CLASSIFICATIONS.add(k._2);
            } else {
                Tuple2<String, String> k2 = new Tuple2<>("CLASS", classification);
                Integer count = countsAsMap.get(k2);
                if (count == null) {
                    PT.put(k, 0.0);
                } else {
                    PT.put(k, ((double) v / (double) count));
                }
            }
        });
        return new Tuple2<>(PT, CLASSIFICATIONS);
    }

    /**
     * 对训练数据的所有元素实现map函数
     */
    private static PairFlatMapFunction<String, Tuple2<String, String>, Integer> str2pair = line -> {
        List<Tuple2<Tuple2<String, String>, Integer>> result = new ArrayList<>();
        String[] tokens = line.split(",");
        // 最后一个是类别，classificationIndex是类别的下标
        int classificationIndex = tokens.length - 1;
        String theClassification = tokens[classificationIndex];

        for (int i = 0; i < (classificationIndex - 1); i++) {
            Tuple2<String, String> k = new Tuple2<>(tokens[i], theClassification);
            result.add(new Tuple2<>(k, 1));
        }
        Tuple2<String, String> k = new Tuple2<>("CLASS", theClassification);
        result.add(new Tuple2<>(k, 1));

        return result.iterator();
    };


}
