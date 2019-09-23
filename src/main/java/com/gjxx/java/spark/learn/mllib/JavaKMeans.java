package com.gjxx.java.spark.learn.mllib;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.regex.Pattern;

/**
 * @Description K-均值算法Spark实现
 * @Author Sxs
 * @Date 2019/9/18 23:01
 * @Version 1.0
 */
public class JavaKMeans {

    public static void main(String[] args) {

        if (args.length < 3) {
            System.err.println("Usage: JavaKMeans <input_file> <k> <max_iterations> [<runs>]");
            System.exit(1);
        }

        String inputFile = args[0];
        int k = Integer.parseInt(args[1]);
        int iterations = Integer.parseInt(args[2]);
        int runs = 1;

        if (args.length >= 4) {
            runs = Integer.parseInt(args[3]);
        }

        JavaSparkContext jsc = new JavaSparkContext("local", "JavaKMeans");
        JavaRDD<String> lines = jsc.textFile(inputFile);
        JavaRDD<Vector> points = lines.map(line2point);

        // 由于 K-Means 算法是迭代计算，这里把数据缓存起来（广播变量）
        points.cache();

        KMeansModel model = KMeans.train(points.rdd(), k, iterations, runs, KMeans.K_MEANS_PARALLEL());

        System.out.println("Cluster centers: ");
        for (Vector center : model.clusterCenters()) {
            System.out.println(" " + center);
        }

        // 输出本次聚类操作的收敛性，此值越低越好
        double cost = model.computeCost(points.rdd());
        System.out.println("Cost: " + cost);

        // 输出每组数据及其所属的子集索引
        points.foreach(point -> System.out.println(model.predict(point) + "->" + point));

        jsc.stop();

    }

    private static Function<String, Vector> line2point = line -> {
        final Pattern SPACE = Pattern.compile(" ");
        String[] tok = SPACE.split(line);
        double[] point = new double[tok.length];
        for (int i = 0; i < tok.length; i++) {
            point[i] = Double.parseDouble(tok[i]);
        }
        return Vectors.dense(point);
    };



}
