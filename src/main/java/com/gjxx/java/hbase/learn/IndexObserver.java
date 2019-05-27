package com.gjxx.java.hbase.learn;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;
import java.util.List;

/**
 * @author Admin
 */
public class IndexObserver extends BaseRegionObserver {

    private HTablePool pool = null;

    @Override
    public void start(CoprocessorEnvironment e) {
        pool = new HTablePool(e.getConfiguration(), 10);
    }

    /**
     * 放入HBase之后，
     * 把数据放入索引表，
     * 进行二级索引的创建
     * @param e 协处理器环境对象
     * @param put put操作
     * @param edit wal
     * @param durability durability
     * @throws IOException IO异常
     */
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        super.postPut(e, put, edit, durability);
    }

    /**
     * 获取数据之前，
     * 先去索引表，查到RowKey，
     * 然后根据RowKey读取数据
     * @param e 协处理器环境对象
     * @param get get操作
     * @param results 返回的结果集
     * @throws IOException IO异常
     */
    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
        super.preGetOp(e, get, results);
    }

    /**
     * 删除之后，
     * 删除索引表中的数据
     * @param e 协处理器环境对象
     * @param delete delete操作
     * @param edit wal
     * @param durability durability
     * @throws IOException IO异常
     */
    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
        super.postDelete(e, delete, edit, durability);
    }
}
