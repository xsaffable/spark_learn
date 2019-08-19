package com.gjxx.java.hbase.learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author Admin
 */
public class Test {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","cdh1,cdh2,cdh3");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        Date start = new Date();

        HTable table = new HTable(conf, "jpstuinfo-test2");

        Put put = new Put(Bytes.toBytes("00004"));
        put.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("stunum"), Bytes.toBytes("wangwu"));
        put.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("inscode"), Bytes.toBytes("12345"));
        put.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("cardtype"), Bytes.toBytes("1234"));
        put.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("idcard"), Bytes.toBytes("1234"));
        put.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("nationality"), Bytes.toBytes("1234"));
        put.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("name"), Bytes.toBytes("1234"));
        put.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("sex"), Bytes.toBytes("1234"));
        put.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("phone"), Bytes.toBytes("1234"));
        put.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("address"), Bytes.toBytes("1234"));
        put.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("photo"), Bytes.toBytes("1234"));
        put.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("fingerprint"), Bytes.toBytes("1234"));
        put.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("busitype"), Bytes.toBytes("1234"));
        put.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("drilicnum"), Bytes.toBytes("1234"));
        put.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("fstdrilicdate"), Bytes.toBytes("1234"));
        put.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("perdritype"), Bytes.toBytes("1234"));
        put.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("traintype"), Bytes.toBytes("1234"));
        put.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("applydate"), Bytes.toBytes("1234"));

        Put put2 = new Put(Bytes.toBytes("00005"));
        put2.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("stunum"), Bytes.toBytes("zhangsan"));
        put2.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("inscode"), Bytes.toBytes("123"));
        put2.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("cardtype"), Bytes.toBytes("1234"));
        put2.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("idcard"), Bytes.toBytes("1234"));
        put2.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("nationality"), Bytes.toBytes("1234"));
        put2.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("name"), Bytes.toBytes("1234"));
        put2.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("sex"), Bytes.toBytes("1234"));
        put2.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("phone"), Bytes.toBytes("1234"));
        put2.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("address"), Bytes.toBytes("1234"));
        put2.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("photo"), Bytes.toBytes("1234"));
        put2.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("fingerprint"), Bytes.toBytes("1234"));
        put2.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("busitype"), Bytes.toBytes("1234"));
        put2.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("drilicnum"), Bytes.toBytes("1234"));
        put2.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("fstdrilicdate"), Bytes.toBytes("1234"));
        put2.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("perdritype"), Bytes.toBytes("1234"));
        put2.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("traintype"), Bytes.toBytes("1234"));
        put2.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("applydate"), Bytes.toBytes("1234"));

        Put put3 = new Put(Bytes.toBytes("00006"));
        put3.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("stunum"), Bytes.toBytes("lisi"));
        put3.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("inscode"), Bytes.toBytes("321"));
        put3.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("cardtype"), Bytes.toBytes("1234"));
        put3.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("idcard"), Bytes.toBytes("1234"));
        put3.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("nationality"), Bytes.toBytes("1234"));
        put3.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("name"), Bytes.toBytes("1234"));
        put3.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("sex"), Bytes.toBytes("1234"));
        put3.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("phone"), Bytes.toBytes("1234"));
        put3.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("address"), Bytes.toBytes("1234"));
        put3.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("photo"), Bytes.toBytes("1234"));
        put3.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("fingerprint"), Bytes.toBytes("1234"));
        put3.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("busitype"), Bytes.toBytes("1234"));
        put3.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("drilicnum"), Bytes.toBytes("1234"));
        put3.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("fstdrilicdate"), Bytes.toBytes("1234"));
        put3.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("perdritype"), Bytes.toBytes("1234"));
        put3.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("traintype"), Bytes.toBytes("1234"));
        put3.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("applydate"), Bytes.toBytes("1234"));

        Put put4 = new Put(Bytes.toBytes("00007"));
        put4.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("stunum"), Bytes.toBytes("xiaoming"));
        put4.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("inscode"), Bytes.toBytes("111222"));
        put4.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("cardtype"), Bytes.toBytes("1234"));
        put4.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("idcard"), Bytes.toBytes("1234"));
        put4.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("nationality"), Bytes.toBytes("1234"));
        put4.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("name"), Bytes.toBytes("1234"));
        put4.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("sex"), Bytes.toBytes("1234"));
        put4.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("phone"), Bytes.toBytes("1234"));
        put4.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("address"), Bytes.toBytes("1234"));
        put4.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("photo"), Bytes.toBytes("1234"));
        put4.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("fingerprint"), Bytes.toBytes("1234"));
        put4.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("busitype"), Bytes.toBytes("1234"));
        put4.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("drilicnum"), Bytes.toBytes("1234"));
        put4.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("fstdrilicdate"), Bytes.toBytes("1234"));
        put4.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("perdritype"), Bytes.toBytes("1234"));
        put4.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("traintype"), Bytes.toBytes("1234"));
        put4.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("applydate"), Bytes.toBytes("1234"));

        List<Put> puts = new ArrayList<>();
        puts.add(put);
        puts.add(put2);
        puts.add(put3);
        puts.add(put4);

        table.put(puts);
        table.flushCommits();

        Date end = new Date();

        System.out.println(end.getTime() - start.getTime());

        table.close();

    }

}
