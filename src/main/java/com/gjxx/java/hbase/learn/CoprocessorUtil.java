package com.gjxx.java.hbase.learn;

import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * @author Admin
 */
@Log4j2
public class CoprocessorUtil {

    public static void loadCoprocessor(String tableName, String jarPath, Class classObj) throws Exception {

        System.out.println("load coprocessor...");

        TableName tName = TableName.valueOf(tableName);
        Path path = new Path(jarPath);
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "cdh1,cdh2,cdh3");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        // disable table
        admin.disableTable(tName);

        HTableDescriptor hTableDescriptor = admin.getTableDescriptor(tName);
        hTableDescriptor.addCoprocessor(classObj.getCanonicalName(), path, Coprocessor.PRIORITY_USER, null);
        hTableDescriptor.setConfiguration("hbase.table.sanity.checks", "false");

        // modify table descriptor
        admin.modifyTable(tName, hTableDescriptor);

        // enable table
        admin.enableTable(tName);

        System.out.println("load coprocessor successful!");

    }

    public static void main(String[] args) throws Exception {
        CoprocessorUtil.loadCoprocessor("buss_stu_info_hou_2", "hdfs://192.168.1.11/SparkLearn-1.0-SNAPSHOT.jar", IndexObserver.class);
    }

}
