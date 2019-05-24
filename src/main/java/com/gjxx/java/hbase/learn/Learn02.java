package com.gjxx.java.hbase.learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Date;

/**
 * @author Admin
 */
public class Learn02 {

    public static void main(String[] args) throws IOException {

        final Date start = new Date();
        System.out.println("start_time = [" + start + "]");

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","cdh1,cdh2,cdh3");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        HTable table = new HTable(conf, "jpstuinfo");
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("baseInfo"));
        scan.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("district"));
        ResultScanner scanner = table.getScanner(scan);
        scan.setCaching(20); // 每次获取20行记录
        scan.setBatch(10); // 每次获取5列

        final long aLong = conf.getLong(HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY, -1);
        System.out.println(aLong);
        try{
            for (Result rs : scanner) {
                final byte[] district = rs.getValue(Bytes.toBytes("baseInfo"), Bytes.toBytes("district"));
                System.out.println("district = [" + Bytes.toString(district) + "]");
            }

        } finally {
            scanner.close();
        }

        table.close();

        final Date end = new Date();
        System.out.println("end_time = [" + end + "]");

        final Long diff = (end.getTime() - start.getTime());
        System.out.println("diff = [" + diff + "]");

    }

}
