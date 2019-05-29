package com.gjxx.java.hbase.learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * @author Admin
 */
public class IndexCop extends BaseRegionObserver {

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        // 得到stunum
        List<Cell> stunumCells = put.get(Bytes.toBytes("baseInfo"), Bytes.toBytes("stunum"));
        String stunum = Bytes.toString(CellUtil.cloneValue(stunumCells.get(0)));

        // 得到inscode
        List<Cell> inscodeCells = put.get(Bytes.toBytes("baseInfo"), Bytes.toBytes("inscode"));
        String inscode = Bytes.toString(CellUtil.cloneValue(inscodeCells.get(0)));

        // 创建一个新的put，用于放入索引表
        Put newPut = new Put(Bytes.toBytes(stunum+"-"+inscode));
        newPut.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("stunum"), Bytes.toBytes(stunum));
        newPut.addColumn(Bytes.toBytes("baseInfo"), Bytes.toBytes("inscode"), Bytes.toBytes(inscode));

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","cdh1,cdh2,cdh3");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        HTable table = new HTable(conf, Bytes.toBytes("buss_stu_info_hou_3_index"));
        table.put(put);
        table.flushCommits();

        table.close();
    }
}
