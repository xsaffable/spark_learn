package com.gjxx.java.spark.learn.movierecommend;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple7;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description 电影推荐 Spark实现，
 *              步骤1: 导入所需的类和接口
 * @Author Sxs
 * @Date 2019/9/16 18:18
 * @Version 1.0
 */
public class MovieRecommendationsWithJoin {

    public static void main(String[] args) {
        // 步骤2: 处理输入参数
        if (args.length < 1) {
            System.err.println("Usage: MovieRecommendationsWithJoin <users-ratings>");
            System.exit(1);
        }
        String usersRatingsInputFile = args[0];
        System.out.println("usersRatingsInputFile=" + usersRatingsInputFile);

        // 步骤3: 创建一个Spark上下文
        JavaSparkContext jsc = new JavaSparkContext("local", "MovieRecommendationsWithJoin");

        // 步骤4: 读取文件并创建RDD
        JavaRDD<String> usersRatings = jsc.textFile(usersRatingsInputFile);

        // 步骤5: 找出谁曾对这个电影评分
        JavaPairRDD<String, Tuple2<String, Integer>> moviesRdd = usersRatings.mapToPair(str2t2);

        // 步骤6: 按movie对moviesRDD分组
        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> moviesGrouped = moviesRdd.groupByKey();

        // 步骤7: 找出每个电影的评分人数
        JavaPairRDD<String, Tuple3<String, Integer, Integer>> usersRdd = moviesGrouped.flatMapToPair(moviesGrouped2Users);

        // 步骤8: 完成自连接
        JavaPairRDD<String, Tuple2<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>> joinedRdd = usersRdd.join(usersRdd);

        // 步骤9: 删除重复(movie1, movie2)对
        JavaPairRDD<String, Tuple2<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>> filteredRdd = joinedRdd.filter(delDuplicate);

        // 步骤10: 生成所有(movie1, movie2)组合
        JavaPairRDD<Tuple2<String, String>, Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> moviePairs = filteredRdd.mapToPair(fil2all);

        // 步骤11: 电影对分组
        JavaPairRDD<Tuple2<String, String>, Iterable<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>>> corrRdd = moviePairs.groupByKey();

        // 步骤12: 计算关联度
        JavaPairRDD<Tuple2<String, String>, Tuple3<Double, Double, Double>> corr = corrRdd.mapValues(MovieRecommendationsWithJoin.corr);

        corr.collect().forEach(System.out::println);

    }

    private static Function<Iterable<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>>,
            Tuple3<Double, Double, Double>> corr = MovieRecommendationsWithJoin::calculateCorrelations;

    /**
     * 计算关联度
     * @param values 各种值
     * @return 关联度三个值
     */
    private static Tuple3<Double, Double, Double> calculateCorrelations(Iterable<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> values) {
        // 各个向量的长度
        int groupSize = 0;
        // ratingProd之和
        int dotProduct = 0;
        // rating1之和
        int rating1Sum = 0;
        // rating2之和
        int rating2Sum = 0;
        // rating1Squared之和
        int rating1NormSq = 0;
        // rating2Squared之和
        int rating2NormSq = 0;
        // numOfRateS1最大值
        int maxNumOfumRaterS1 = 0;
        // numOfRaterS2最大值
        int maxNumOfumRaterS2 = 0;
        for (Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> t7 : values) {
            groupSize++;
            dotProduct += t7._5();
            rating1Sum += t7._1();
            rating2Sum += t7._3();
            rating1NormSq += t7._6();
            rating2NormSq += t7._7();
            int numOfRaterS1 = t7._2();
            if (numOfRaterS1 > maxNumOfumRaterS1) {
                maxNumOfumRaterS1 = numOfRaterS1;
            }
            int numOfRaterS2 = t7._4();
            if (numOfRaterS2 > maxNumOfumRaterS2) {
                maxNumOfumRaterS2 = numOfRaterS2;
            }
        }
        double pearson = calculatePersonCorrelation(groupSize, dotProduct, rating1Sum, rating2Sum, rating1NormSq, rating2NormSq);
        double cosine = calculateCosineCorrelation(dotProduct, Math.sqrt(rating1NormSq), Math.sqrt(rating2NormSq));
        double jaccard = calculateJaccardCorrelation(groupSize, maxNumOfumRaterS1, maxNumOfumRaterS2);
        return new Tuple3<>(pearson, cosine, jaccard);
    }

    /**
     * 计算杰卡德关联度
     * @param inCommon 各个向量的长度
     * @param totalA numOfRateS1最大值
     * @param totalB numOfRateS2最大值
     * @return 杰卡德相似度
     */
    private static double calculateJaccardCorrelation(double inCommon,
                                                      double totalA,
                                                      double totalB) {
        double union = totalA + totalB - inCommon;
        return inCommon / union;
    }

    /**
     * 计算余弦关联度
     * @param dotProduct ratingProd之和
     * @param rating1Norm Math.sqrt(rating1NormSq)
     * @param rating2Norm Math.sqrt(rating2NormSq)
     * @return 余弦关联度
     */
    private static double calculateCosineCorrelation(double dotProduct,
                                                     double rating1Norm,
                                                     double rating2Norm) {
        return dotProduct / (rating1Norm * rating2Norm);
    }

    /**
     * 计算皮尔逊关联度
     * @param size 各个向量的长度
     * @param dotProduct ratingProd之和
     * @param rating1Sum rating1之和
     * @param rating2Sum rating2之和
     * @param rating1NormSq rating1Squared之和
     * @param rating2NormSq rating2Squared之和
     * @return 皮尔逊关联度
     */
    private static double calculatePersonCorrelation(double size,
                                                     double dotProduct,
                                                     double rating1Sum,
                                                     double rating2Sum,
                                                     double rating1NormSq,
                                                     double rating2NormSq) {
        double numerator = size * dotProduct - rating1Sum * rating2Sum;
        double denominator =
                Math.sqrt(size * rating1NormSq - rating1Sum * rating1Sum) *
                Math.sqrt(size * rating2NormSq - rating2Sum * rating2Sum);
        return numerator / denominator;
    }

    /**
     * 生成所有(movie1, movie2)组合
     */
    private static PairFunction<Tuple2<String, Tuple2<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>>,
            Tuple2<String, String>, Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> fil2all = t2 -> {
        Tuple3<String, Integer, Integer> movie1 = t2._2._1;
        Tuple3<String, Integer, Integer> movie2 = t2._2._2;
        Tuple2<String, String> m1m2Key = new Tuple2<>(movie1._1(), movie2._1());
        int ratingProduct = movie1._2() * movie2._2();
        int rating1Squared = movie1._2() * movie1._2();
        int rating2Squared = movie2._2() * movie2._2();

        Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> t7 =
                new Tuple7<>(movie1._2(), movie1._3(), movie2._2(), movie2._3(), ratingProduct, rating1Squared, rating2Squared);

        return new Tuple2<>(m1m2Key, t7);
    };

    /**
     * 删除重复(movie1, movie2)对
     */
    private static Function<Tuple2<String, Tuple2<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>>, Boolean> delDuplicate = t2 -> {
        String moivieName1 = t2._2._1._1();
        String movieName2 = t2._2._2._1();
        return moivieName1.compareTo(movieName2) < 0;
    };

    /**
     * 找出谁曾对这个电影评分
     * k: movie
     * v: (user, rating)
     */
    private static PairFunction<String, String, Tuple2<String, Integer>> str2t2 = str -> {
        String[] record = str.split(" ");
        String user = record[0];
        String movie = record[1];
        Integer rating = new Integer(record[2]);
        Tuple2<String, Integer> userAndRating = new Tuple2<>(user, rating);
        return new Tuple2<>(movie, userAndRating);
    };

    /**
     * 找出每个电影的评分人数
     */
    private static PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<String, Integer>>>, String, Tuple3<String, Integer, Integer>> moviesGrouped2Users = t2 -> {
        List<Tuple2<String, Integer>> listOfUsersAndRatings = new ArrayList<>();
        String movie = t2._1;
        Iterable<Tuple2<String, Integer>> pairsOfUserAndRating = t2._2;
        int numberOfRaters = 0;
        for (Tuple2<String, Integer> pairOfUserAndRating : pairsOfUserAndRating) {
            numberOfRaters++;
            listOfUsersAndRatings.add(pairOfUserAndRating);
        }

        List<Tuple2<String, Tuple3<String, Integer, Integer>>> results = new ArrayList<>();
        for (Tuple2<String, Integer> userAndRating : listOfUsersAndRatings) {
            String user = userAndRating._1;
            Integer rating = userAndRating._2;
            Tuple3<String, Integer, Integer> t3 = new Tuple3<>(movie, rating, numberOfRaters);
            results.add(new Tuple2<>(user, t3));
        }
        return results.iterator();
    };

}
