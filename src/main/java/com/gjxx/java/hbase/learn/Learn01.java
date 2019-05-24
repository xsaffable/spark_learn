package com.gjxx.java.hbase.learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Date;

/**
 * @author Admin
 */
public class Learn01 {

    public static void main(String[] args) throws IOException {

        final Date start = new Date();
        System.out.println("start_time = [" + start + "]");

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","cdh1,cdh2,cdh3");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        HTable table = new HTable(conf, "jpstuinfo");
        Get get = new Get(Bytes.toBytes("20190403_120110_1232451037138903_8421354498962168_120113198902185624"));
        get.addFamily(Bytes.toBytes("baseInfo"));
        get.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("district"));
        Result result = table.get(get);
        byte[] district = result.getValue(Bytes.toBytes("baseInfo"), Bytes.toBytes("district"));
        System.out.println(Bytes.toString(district));
        boolean con = result.containsColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("idcard"));
        System.out.println(con);

        final Date end = new Date();
        System.out.println("end_time = [" + end + "]");

        final Long diff = (end.getTime() - start.getTime());
        System.out.println("diff = [" + diff + "]");

    }

}
