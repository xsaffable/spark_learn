package com.gjxx.java.hbase.learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Learn03 {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","cdh1,cdh2,cdh3");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        HTable table = new HTable(conf, Bytes.toBytes("jpstuinfo"));
        PageFilter filter = new PageFilter(15);

        int totalRows = 0;
        byte[] lastRow = null;
        while(true) {
            Scan scan = new Scan();
            scan.setFilter(filter);
            if (lastRow != null) {
                byte[] startRow = Bytes.add(lastRow, new byte[]{});
                System.out.println("start row: = [" + Bytes.toString(startRow) + "]");
                scan.setStartRow(startRow);
            }
            ResultScanner scanner = table.getScanner(scan);
            int localRows = 0;
            Result result;
            while ((result = scanner.next()) != null) {
                System.out.println(localRows++ + ":" + result + "]");
                totalRows++;
                lastRow = result.getRow();
            }
            scanner.close();
            if (localRows == 0) {
                break;
            }
        }
        System.out.println("total rows: " + totalRows);

        table.close();
    }

}
