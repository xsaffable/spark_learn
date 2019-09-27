package com.gjxx.java.spark.learn.mllib;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author Sxs
 * @description 使用朴素贝叶斯分类器，对新数据分类
 * @date 2019/9/26 18:35
 */
public class NaiveBayesClassifier implements Serializable {

    public static void main(String[] args) {
        // 步骤2: 处理输入参数
        if (args.length != 3) {
            System.err.println("Usage: NaiveBayesClassifier <input-data-filename> <NB-PT-path>");
            System.exit(1);
        }
        // 要分类的数据
        final String inputDataFilename = args[0];
        // 分类器路径
        final String nbProbabilityTablePath = args[1];
        final String classPath = args[2];

        // 步骤3: 创建一个Spark上下文对象
        JavaSparkContext jsc = new JavaSparkContext("local", "NaiveBayesClassifier");

        // 步骤4: 读取要分类的新数据
        JavaRDD<String> newData = jsc.textFile(inputDataFilename);

        // 步骤5: 读取分类器
        JavaRDD<String> nbc = jsc.textFile(nbProbabilityTablePath);
        JavaPairRDD<Tuple2<String, String>, Double> classifierRDD = nbc.mapToPair(str2classifier);

        // 步骤6: 缓存分类器组件，集群中任何节点都可以使用这些组件
        Broadcast<Map<Tuple2<String, String>, Double>> broadcastClassifier = jsc.broadcast(classifierRDD.collectAsMap());
        JavaRDD<String> classRDD = jsc.textFile(classPath);
        Broadcast<List<String>> broadcastClasses = jsc.broadcast(classRDD.collect());

        // 步骤7: 对新数据分类
        JavaPairRDD<String, String> classified = newData.mapToPair(line -> {
            // 从Spark缓存得到分类器
            Map<Tuple2<String, String>, Double> CLASSIFIER = broadcastClassifier.value();
            // 从Spark缓存得到类
            List<String> CLASSES = broadcastClasses.value();

            String[] attributes = line.split(",");
            String selectedClass = null;
            double maxPosterior = 0.0;

            for (String aClass : CLASSES) {
                Double posterior = CLASSIFIER.get(new Tuple2<>("CLASS", aClass));
                for (String attribute : attributes) {
                    Double probability = CLASSIFIER.get(new Tuple2<>(attribute, aClass));
                    if (probability == null) {
//                        posterior = 0.0;
//                        break;
                    } else {
                        posterior *= probability;
                    }
                }
                if (selectedClass == null) {
                    // 计算第一个分类的值
                    selectedClass = aClass;
                    maxPosterior = posterior;
                } else {
                    if (posterior > maxPosterior) {
                        selectedClass = aClass;
                        maxPosterior = posterior;
                    }
                }

            }
            return new Tuple2<>(line, selectedClass);

        });

        classified.collect().forEach(System.out::println);

        jsc.stop();
    }

    /**
     * 读取分类器
     */
    private static PairFunction<String, Tuple2<String, String>, Double> str2classifier = line -> {
        String[] tokens1 = line.split("\t");
        String[] tokens2 = tokens1[0].split(",");
        Tuple2<String, String> fieldAndClass = new Tuple2<>(tokens2[0].substring(1), tokens2[1].substring(0, tokens2[1].length() - 1));
        return new Tuple2<>(fieldAndClass, Double.valueOf(tokens1[1]));
    };

}
