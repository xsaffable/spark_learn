package com.gjxx.java.spark.learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author Admin
 */
public class SparkLearn2 {

    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext("local[4]", "SparkLearn2");

        Configuration hConf = HBaseConfiguration.create();
        hConf.set("hbase.zookeeper.quorum","cdh1,cdh2,cdh3");
        hConf.set("hbase.zookeeper.property.clientPort", "2181");
        JavaHBaseContext hc = new JavaHBaseContext(jsc, hConf);

        String tableName = "jpstuinfo";
        String family = "baseInfo";
        String[] cols = new String[]{"createtime", "district", "inscode", "schname", "sex", "idcard"};

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(family));
        for (String col : cols) {
            scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(col));
        }

        JavaRDD<Tuple2<ImmutableBytesWritable, Result>> hbaseRDD = hc.hbaseRDD(TableName.valueOf(tableName), scan);

        final JavaRDD<List> rdd = hbaseRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, List>() {
            @Override
            public List call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {
                Result values = v1._2;
                // 时间
                String createtime = Bytes.toString(values.getValue(Bytes.toBytes("baseInfo"), Bytes.toBytes("createtime"))).substring(0, 10);
                // 地区编码
                String district = Bytes.toString(values.getValue(Bytes.toBytes("baseInfo"), Bytes.toBytes("district")));
                // 省份编码
                String procode = "100000";
                // 城市编码
                String citycode = "100000";
                if (!(district.length() == 0)) {
                    procode = district.substring(0, 2) + "0000";
                    citycode = district.substring(0, 4) + "00";
                }
                // 驾校编码
                String inscode = Bytes.toString(values.getValue(Bytes.toBytes("baseInfo"), Bytes.toBytes("inscode")));
                // 驾校名称
                String schname = Bytes.toString(values.getValue(Bytes.toBytes("baseInfo"), Bytes.toBytes("schname")));
                // kpi=1 -> 学员报名人数
                int kpi = 1;
                return Arrays.asList(createtime, procode, citycode, inscode, schname, kpi);
            }
        });

        final JavaPairRDD<List, Integer> rdd2 = rdd.mapToPair(new PairFunction<List, List, Integer>() {
            @Override
            public Tuple2<List, Integer> call(List list) throws Exception {
                return new Tuple2<>(list, 1);
            }
        });

        final JavaPairRDD<List, Integer> rdd3 = rdd2.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        rdd3.foreach(line -> System.out.println("line = [" + line + "]"));


    }

}
