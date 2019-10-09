package com.gjxx.java.spark.learn.kmer;

import com.google.common.base.Preconditions;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

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

        // 步骤7: 生成K-mers

        // 步骤8: 组合/归约多次出现的K-mers

        // 步骤9: 为所有分区创建一个本地top N

        // 步骤10: 收集所有分区的本地top N，并从中找到最终的top N

        // 步骤11: 按降序发出最终的top N

        jsc.close();
    }

    private static JavaSparkContext createJavaSparkContext() throws Exception {
        return new JavaSparkContext("local", "Kmer");
    }

}
