package com.gjxx.java.spark.learn.utils;

/**
 * HBase to HBase 工具类
 * @author Admin
 */
public class H2hUtil {

    private final static String FAMILY = "baseInfo";

//    public static void run(JavaHBaseContext jhc, Class<?> clz, String originTable, String destTable, String... rowCondition) {
//        // 创建scan扫描器
//        Scan scan = new Scan();
//
//        // 添加要扫描的列
//        scan.addFamily(Bytes.toBytes(FAMILY));
//
//        // 根据所给的实体类，添加要扫描的列
//        Field[] fields = clz.getDeclaredFields();
//        for (Field field : fields) {
//            String fieldName = field.getName();
//            scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(fieldName));
//        }
//
//        // 读取指定表的全部数据
//        JavaRDD<Tuple2<ImmutableBytesWritable, Result>> hBaseRdd = jhc.hbaseRDD(TableName.valueOf(originTable), scan);
//
//        // 批量写入到另一个表
//        jhc.bulkPut(hBaseRdd, TableName.valueOf(destTable), line -> {
//
//            Object obj = HBaseUtil.result2Obj(line._2, Student.class);
//
//            return HBaseUtil.obj2Put(obj, rowCondition);
//        });
//
//    }

}
