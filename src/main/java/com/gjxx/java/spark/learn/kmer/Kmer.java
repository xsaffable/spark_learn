package com.gjxx.java.spark.learn.kmer;

import com.google.common.base.Preconditions;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author Sxs
 * @description K-mer 计数
 * @date 2019/10/8 18:58
 */
public class Kmer {

    public static void main(String[] args) throws Exception {
        // 步骤2: 处理输入参数
        Preconditions.checkArgument(args.length >= 3, "Usage: Kmer <fastq-file> <K> <N>");
        // FASTQ文件作为输入
        final String fastqFileName = args[0];
        // 查找K-mers
        final int K = Integer.parseInt(args[1]);
        // 查找top N
        final int N = Integer.parseInt(args[2]);

        // 步骤3: 创建一个Spark上下文对象
        JavaSparkContext jsc = createJavaSparkContext();

        // 步骤4: 广播K和N作为全局共享对象
        Broadcast<Integer> broadcastK = jsc.broadcast(K);
        Broadcast<Integer> broadcastN = jsc.broadcast(N);

        // 步骤5: 从HDFS读取FASTQ文件，并创建第一个RDD
        JavaRDD<String> records = jsc.textFile(fastqFileName);
        records.collect().forEach(System.out::println);

        // 步骤6: 过滤冗余的记录
        JavaRDD<String> filteredRdd = records.filter(record -> {
            String firstChar = record.substring(0, 1);
            return !("@".equals(firstChar)
                    || "+".equals(firstChar)
                    || ";".equals(firstChar)
                    || "!".equals(firstChar)
                    || "~".equals(firstChar));
        });

        // 步骤7: 生成K-mers
        JavaPairRDD<String, Integer> kmers = filteredRdd.flatMapToPair(record -> {
            int k = broadcastK.getValue();
            List<Tuple2<String, Integer>> list = new ArrayList<>();
            for (int i = 0; i < record.length() - k + 1; i++) {
                String kmer = record.substring(i, k + i);
                list.add(new Tuple2<>(kmer, 1));
            }
            return list.iterator();
        });

        // 步骤8: 组合/归约多次出现的K-mers
        JavaPairRDD<String, Integer> kmersGrouped = kmers.reduceByKey(Integer::sum);

        // 步骤9: 为所有分区创建一个本地top N
        // 步骤10: 收集所有分区的本地top N，并从中找到最终的top N
        int n = broadcastN.getValue();
        List<Tuple2<String, Integer>> finalTopn = kmersGrouped.top(n, TupleComparatorDescending.INSTANCE);

        // 步骤11: 按降序发出最终的top N
        finalTopn.forEach(System.out::println);

        jsc.close();
    }

    static class TupleComparatorAscending implements Comparator<Tuple2<String, Integer>>, Serializable {

        final static TupleComparatorAscending INSTANCE = new TupleComparatorAscending();

        @Override
        public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
            return o2._2.compareTo(o1._2);
        }
    }

    static class TupleComparatorDescending implements Comparator<Tuple2<String, Integer>>, Serializable {

        final static TupleComparatorDescending INSTANCE = new TupleComparatorDescending();

        @Override
        public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
            return -o2._2.compareTo(o1._2);
        }
    }

    private static JavaSparkContext createJavaSparkContext() throws Exception {
        return new JavaSparkContext("local", "Kmer");
    }

}
