package com.gjxx.java.hbase_learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class Learn04 {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","cdh1,cdh2,cdh3");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
//        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("student1"));
//        HColumnDescriptor coldef = new HColumnDescriptor("baseInfo");
//        desc.addFamily(coldef);
//        hBaseAdmin.createTable(desc);

        HTableDescriptor[] htds = hBaseAdmin.listTables();
        for (HTableDescriptor htd : htds) {
            System.out.println(htd);
        }

        hBaseAdmin.close();


    }

}
