package com.gjxx.java.spark.learn.markov;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @Description 使用马尔可夫模型的智能邮件营销
 *              步骤0: 导入所需的类和接口
 * @Author Sxs
 * @Date 2019/9/18 19:57
 * @Version 1.0
 */
public class SparkMarkov {

    private static final int RECORD_LENGTH = 4;

    public static void main(String[] args) {

        // 步骤1: 处理输入参数
        if (args.length != 1) {
            System.err.println("Usage: SparkMarkov <input-path>");
            System.exit(1);
        }
        final String inputPath = args[0];
        System.out.println("inputPath:args[0]=" + args[0]);

        // 步骤2: 创建上下文对象，并把输入转换为JavaRDD<String>，其中各个元素分别是一个输入记录
        JavaSparkContext jsc = new JavaSparkContext("local", "SparkMarkov");
        JavaRDD<String> records = jsc.textFile(inputPath);

        // 步骤3: 将JavaRDD<String>转换为JavaPairRDD<K, V>，其中
        // K: customerID
        // V: Tuple<purchaseDate, Amount> : Tuple2<Long, Integer>
        JavaPairRDD<String, Tuple2<Long, Integer>> kvs = records.mapToPair(records2Pair);

        // 步骤4: 按customerID对交易分组：应用groupByKey()
        JavaPairRDD<String, Iterable<Tuple2<Long, Integer>>> customerRdd = kvs.groupByKey();

        // 步骤5: 创建马尔可夫状态序列：State1, State2, ..., StateN
        JavaPairRDD<String, List<String>> stateSequence = customerRdd.mapValues(customer2state);

        // 步骤6: 生成马尔可夫状态转移矩阵
        JavaPairRDD<Tuple2<String, String>, Integer> model = stateSequence.flatMapToPair(state2model);

        JavaPairRDD<Tuple2<String, String>, Integer> markovModel = model.reduceByKey(Integer::sum);

        // 步骤7: 发出最终输出
        markovModel.collect().forEach(System.out::println);

        jsc.stop();

    }

    /**
     * 生成马尔可夫状态转移矩阵
     */
    private static PairFlatMapFunction<Tuple2<String, List<String>>, Tuple2<String, String>, Integer> state2model = t2 -> {
        List<String> states = t2._2;
        if ((states == null) || (states.size() < 2)) {
            return Collections.emptyIterator();
        }
        List<Tuple2<Tuple2<String, String>, Integer>> mapperOutput = new ArrayList<>();
        for (int i = 0; i < (states.size() - 1); i++) {
            String fromState = states.get(i);
            String toState = states.get(i + 1);
            Tuple2<String, String> k = new Tuple2<>(fromState, toState);
            mapperOutput.add(new Tuple2<>(k, 1));

            Tuple2<String, String> kAll = new Tuple2<>("ALL", "ALL");
            mapperOutput.add(new Tuple2<>(kAll, 1));
        }
        return mapperOutput.iterator();
    };

    /**
     * 创建马尔可夫状态序列：State1, State2, ..., StateN
     */
    private static Function<Iterable<Tuple2<Long, Integer>>, List<String>> customer2state = iter -> {
        List<Tuple2<Long, Integer>> list = toList(iter);
        list.sort(TupleComparatorAscending.INSTANCE);
        // 将有序列表(按日期)转换为一个状态序列
        return toStateSequence(list);
    };

    private static PairFunction<String, String, Tuple2<Long, Integer>> records2Pair = record -> {
        String[] tokens = StringUtils.split(record, ",");
        if (tokens.length != RECORD_LENGTH) {
            // 不是正确格式
            return null;
        }
        long date = 0;
        try {
            date = DateUtils.parseDate(tokens[2], new String[]{"yyyy-MM-dd"}).getTime();
        } catch (Exception e) {
            e.printStackTrace();
            // 暂时忽略这一项
        }
        int amount = Integer.parseInt(tokens[3]);
        Tuple2<Long, Integer> v = new Tuple2<>(date, amount);

        return new Tuple2<>(tokens[0], v);
    };

    /**
     * 将一个Iterable<Tuple2<Long, Integer>>转换为List<Tuple2<Long, Integer>>
     * @param iter Iterable<Tuple2<Long, Integer>>
     * @return List<Tuple2<Long, Integer>>
     */
    private static List<Tuple2<Long, Integer>> toList(Iterable<Tuple2<Long, Integer>> iter) {
        List<Tuple2<Long, Integer>> list = new ArrayList<>();
        for (Tuple2<Long, Integer> element : iter) {
            list.add(element);
        }
        return list;
    }

    /**
     * 将一个有序的交易序列转换为一个状态序列
     * @param list 有序的交易序列
     * @return 状态序列
     */
    private static List<String> toStateSequence(List<Tuple2<Long, Integer>> list) {
        if (list.size() < 2) {
            // 没有足够的数据
            return null;
        }
        List<String> stateSequence = new ArrayList<>();
        // 前一个状态
        Tuple2<Long, Integer> prior = list.get(0);
        for (int i = 1; i < list.size(); i++) {
            // 现在的状态
            Tuple2<Long, Integer> current = list.get(i);

            Long priorDate = prior._1;
            Long date = current._1;
            // 1天 = 24 * 60 * 60 * 1000 毫秒
            long daysDiff = (date - priorDate) / (24 * 60 * 60 * 1000);
            int priorAmount = prior._2;
            int amount = current._2;
            int amountDiff = amount - priorAmount;

            String dd = null;
            if (daysDiff < 30) {
                dd = "S";
            } else if (daysDiff < 60) {
                dd = "M";
            } else {
                dd  ="L";
            }

            String ad = null;
            if (priorAmount < 0.9 * amount) {
                ad = "L";
            } else if (priorAmount < 1.1 * amount) {
                ad = "E";
            } else {
                ad = "G";
            }

            String element = dd + ad;
            stateSequence.add(element);
            prior = current;

        }
        return stateSequence;
    }

    /**
     * 元组比较器
     */
    private static class TupleComparatorAscending implements Comparator<Tuple2<Long, Integer>>, Serializable {

        final static TupleComparatorAscending INSTANCE = new TupleComparatorAscending();

        @Override
        public int compare(Tuple2<Long, Integer> o1, Tuple2<Long, Integer> o2) {
            // 根据时间升序排序列
            return o1._1.compareTo(o2._1);
        }

    }

}
