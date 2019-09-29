package com.gjxx.java.spark.learn.triangle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author Sxs
 * @description 查找图中所有的三角形
 * @date 2019/9/29 18:24
 */
public class CountTriangles {

    public static void main(String[] args) {

        // 步骤2: 读取和验证输入参数
        if (args.length < 1) {
            System.err.println("Usage: CountTriangles <hdfs-file>");
            System.exit(1);
        }
        String hdfsFile = args[0];

        // 步骤3: 创建一个JavaSparkContext对象
        JavaSparkContext jsc = new JavaSparkContext("local", "CountTriangles");

        // 步骤4: 读取表示一个图的输入文件
        JavaRDD<String> lines = jsc.textFile(hdfsFile);

        // 步骤5: 为所有边创建一个新的JavaPairRDD，包括(source, destination)和(destination, source)
        JavaPairRDD<Long, Long> edges = lines.flatMapToPair(line -> {
            String[] nodes = line.split(" ");
            long start = Long.parseLong(nodes[0]);
            long end = Long.parseLong(nodes[1]);
            // 这里是无向图，所以边都是相互的
            return Arrays.asList(new Tuple2<>(start, end), new Tuple2<>(end, start)).iterator();
        });

        // 步骤6: 创建一个新的JavaPairRDD，将生成三联体
        JavaPairRDD<Long, Iterable<Long>> triads = edges.groupByKey();

        // 步骤7: 创建一个新的JavaPairRDD，表示所有可能的三联体
        JavaPairRDD<Tuple2<Long, Long>, Long> possibleTriads = triads.flatMapToPair(t2 -> {
            Iterable<Long> values = t2._2;
            List<Tuple2<Tuple2<Long, Long>, Long>> result = new ArrayList<>();

            // 生成可能的三联体
            for (Long value : values) {
                Tuple2<Long, Long> k2 = new Tuple2<>(t2._1, value);
                Tuple2<Tuple2<Long, Long>, Long> k2v2 = new Tuple2<>(k2, 0L);
                result.add(k2v2);
            }

            List<Long> valuesCopy = new ArrayList<>();
            for (Long value : values) {
                valuesCopy.add(value);
            }
            Collections.sort(valuesCopy);

            // 生成可能的三联体
            for (int i = 0; i < valuesCopy.size() - 1; i++) {
                for (int j = i + 1; j < valuesCopy.size(); j++) {
                    Tuple2<Long, Long> k2 = new Tuple2<>(valuesCopy.get(i), valuesCopy.get(j));
                    Tuple2<Tuple2<Long, Long>, Long> k2v2 = new Tuple2<>(k2, t2._1);
                    result.add(k2v2);
                }
            }

            return result.iterator();

        });

        // 步骤8: 创建一个新的JavaPairRDD，将生成三角形
        JavaPairRDD<Tuple2<Long, Long>, Iterable<Long>> triadsGrouped = possibleTriads.groupByKey();

        // 步骤9: 创建一个新的JavaPairRDD，表示所有的三角形
        JavaRDD<Tuple3<Long, Long, Long>> triangelesWithDuplicates = triadsGrouped.flatMap(t2 -> {
            Tuple2<Long, Long> key = t2._1;
            Iterable<Long> values = t2._2;

            // 这里假设没有ID为0的节点

            List<Long> list = new ArrayList<>();
            boolean haveSeenSpecialNodeZero = false;
            for (Long node : values) {
                if (node == 0) {
                    haveSeenSpecialNodeZero = true;
                } else {
                    list.add(node);
                }
            }

            List<Tuple3<Long, Long, Long>> result = new ArrayList<>();
            if (haveSeenSpecialNodeZero) {
                if (list.isEmpty()) {
                    // 没有找到三角形
                    return result.iterator();
                }
                // 发出三角形
                for (Long node : list) {
                    long[] aTriangle = {key._1, key._2, node};
                    Arrays.sort(aTriangle);
                    Tuple3<Long, Long, Long> t3 = new Tuple3<>(aTriangle[0], aTriangle[1], aTriangle[2]);
                    result.add(t3);
                }
            } else {
                // 没有找到三角形
                return result.iterator();
            }

            return result.iterator();

        });

        // 步骤10: 消除重复的三角形，创建唯一的三角形

        JavaRDD<Tuple3<Long, Long, Long>> uniqueTriangles = triangelesWithDuplicates.distinct();

        uniqueTriangles.collect().forEach(System.out::println);

        jsc.stop();
    }

}
