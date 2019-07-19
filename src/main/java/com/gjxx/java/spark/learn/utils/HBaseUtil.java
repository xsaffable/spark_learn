package com.gjxx.java.spark.learn.utils;

import com.gjxx.java.spark.learn.conf.HBaseConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Properties;

/**
 * @author Admin
 */
public class HBaseUtil {

    /**
     * hbase列族
     */
    private final static String FAMILY = HBaseConf.FAMILY;

    /**
     * 设置hbase相关配置
     */
    private static Configuration conf = HBaseConfiguration.create();

    static {
        InputStream stream = HBaseUtil.class.getClassLoader().getResourceAsStream("hbase.properties");
        Properties prop = new Properties();
        try {
            prop.load(stream);
            conf.set("hbase.zookeeper.quorum", prop.getProperty("hbase.zookeeper.quorum"));
            conf.set("hbase.zookeeper.property.clientPort", prop.getProperty("hbase.zookeeper.property.clientPort"));
            conf.setInt("hbase.client.operation.timeout", 60000 * 10);
            conf.setInt("hbase.rpc.timeout", 60000 * 10);
            conf.setInt("hbase.client.scanner.timeout.period", 60000 * 10);
            conf.setInt("mapreduce.task.timeout", 60000 * 10);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取hbase连接
     *
     * @return 返回conn对象
     * @throws IOException IOException
     */
    public static Connection getConn() throws IOException {
        return ConnectionFactory.createConnection(conf);
    }

    /**
     * 获得JavaHBaseContext
     *
     * @param jsc JavaSparkContext
     * @return JavaHBaseContext
     */
    public static JavaHBaseContext getHC(JavaSparkContext jsc) {
        return new JavaHBaseContext(jsc, conf);
    }

    /**
     * 根据clz，把put中的value取出，
     * 转换成object
     *
     * @param clz Class<?>
     * @param put Put
     * @return Object
     */
    public static Object put2Obj(Class<?> clz, Put put) {
        Object o = null;
        if (clz != null) {
            // 获得该类的所有属性
            Field[] fields = clz.getDeclaredFields();
            try {
                // 创建一个对象
                o = clz.newInstance();
                for (Field field : fields) {
                    List<Cell> cells = put.get(Bytes.toBytes(FAMILY), Bytes.toBytes(field.getName()));
                    // 当小于1时，证明put中没有该属性值
                    if (cells.size() >= 1) {
                        byte[] value = CellUtil.cloneValue(cells.get(0));
                        // 设置属性可访问
                        field.setAccessible(true);
                        field.set(o, Bytes.toString(value));
                    }
                }
            } catch (InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
            }
            return o;
        } else { // clz未输入
            System.err.println("参数输入有误");
            System.exit(0);
            return o;
        }
    }

    /**
     * 把object根据rowCondition，转换成put
     *
     * @param o            Object
     * @param rowCondition String[]
     * @return Put
     */
    public static Put obj2Put(Object o, String... rowCondition) {
        // 获得object的所有属性
        Class<?> clz = o.getClass();
        Field[] fields = clz.getDeclaredFields();

        // 使用StringBuffer提高性能
        StringBuffer rowKey = new StringBuffer();

        // 第一次遍历，获得rowKey
        if (rowCondition.length > 0) {
            for (String rc : rowCondition) {
                for (Field field : fields) {
                    if (rc.equals(field.getName())) {
                        try {
                            // 设置属性可访问
                            field.setAccessible(true);
                            rowKey.append("_");
                            rowKey.append(field.get(o));
                            break;
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

        // 如果为空，则使用默认值(第一个属性值)
        if (rowKey.length() <= 0) {
            try {
                // 设置属性可访问
                fields[0].setAccessible(true);
                rowKey.append("_");
                rowKey.append(fields[0].get(o));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        // 去掉第一个 _
        String row = rowKey.substring(1);

        // 以row作为rowKey创建put
        Put put = new Put(Bytes.toBytes(row));

        // 第二次遍历，取出所有的有效值，存入put
        for (Field field : fields) {
            try {
                // 设置属性可访问
                field.setAccessible(true);
                String fieldName = field.getName();
                Object value = field.get(o);
                // 如果为null，则不存入put
                if (value != null) {
                    put.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(fieldName), Bytes.toBytes(value.toString()));
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }

        }

        return put;
    }

    /**
     * put根据给出的clz和condition，
     * 转换成新的put
     *
     * @param put          Put
     * @param clz          Class<?> put的变量格式
     * @param rowCondition String[]
     * @return Put
     */
    public static Put put2put(Put put, Class<?> clz, String... rowCondition) {
        // 使用StringBuffer提高性能
        StringBuffer rowKey = new StringBuffer();
        if (rowCondition.length > 0) {
            for (String rc : rowCondition) {
                List<Cell> cells = put.get(Bytes.toBytes(FAMILY), Bytes.toBytes(rc));
                if (cells.size() >= 1) {
                    byte[] value = CellUtil.cloneValue(cells.get(0));
                    rowKey.append("_");
                    rowKey.append(Bytes.toString(value));
                }
            }
        }
        // 如果为空，则使用默认值(原来的rowKey)
        if (rowKey.length() <= 0) {
            rowKey.append("_");
            rowKey.append(Bytes.toString(put.getRow()));
        }
        // 去掉第一个 _
        String row = rowKey.substring(1);
        Put newPut = new Put(Bytes.toBytes(row));

        if (clz != null) {
            // 获得该类的所有属性
            Field[] fields = clz.getDeclaredFields();
            for (Field field : fields) {
                List<Cell> cells = put.get(Bytes.toBytes(FAMILY), Bytes.toBytes(field.getName()));
                // 当小于1时，证明put中没有该属性值
                if (cells.size() >= 1) {
                    byte[] value = CellUtil.cloneValue(cells.get(0));
                    newPut.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(field.getName()), value);
                }
            }
        } else { // clz未输入
            System.err.println("参数输入有误");
            System.exit(0);
        }
        return newPut;
    }

    /**
     * 根据Result转换成Object
     * @param result Result
     * @param clz Class<?>
     * @return Object
     */
    public static Object result2Obj(Result result, Class<?> clz) {
        Object o = null;
        if (clz != null) {
            // 获得该类的所有属性
            Field[] fields = clz.getDeclaredFields();
            try {
                // 创建一个对象
                o = clz.newInstance();
                for (Field field : fields) {
                    byte[] value = result.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(field.getName()));
                    if (value != null) {
                        // 设置属性可访问
                        field.setAccessible(true);
                        field.set(o, Bytes.toString(value));
                    }
                }
            } catch (InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
            }
            return o;
        } else { // clz未输入
            System.err.println("参数输入有误");
            System.exit(0);
            return o;
        }
    }

    /**
     * result to put
     * 保持原来的rowKey
     * @param result Result
     * @param clz Class<?>
     * @return Put
     */
    public static Put result2Put(Result result, Class<?> clz) {
        Put put = null;
        if (clz != null) {
            // 获得该类的所有属性
            Field[] fields = clz.getDeclaredFields();
            // 创建一个put
            put = new Put(result.getRow());
            for (Field field : fields) {
                String fieldName = field.getName();
                byte[] value = result.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(fieldName));
                if (value != null) {
                    put.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(fieldName), value);
                }
            }
        }
        return put;
    }

    /**
     * 读取指定表的全部数据
     * @param clz Class<?>
     * @param jhc JavaHBaseContext
     * @param originTable String
     * @param rowRange String[] 两个参数， start/stop
     * @return JavaRDD<Tuple2<ImmutableBytesWritable, Result>>
     */
    public static JavaRDD<Tuple2<ImmutableBytesWritable, Result>> readTable(Class<?> clz, JavaHBaseContext jhc, String originTable, String... rowRange) {
        // 创建scan扫描器
        Scan scan = new Scan();

        // 添加要扫描的列
        scan.addFamily(Bytes.toBytes(FAMILY));

        // 根据所给的实体类，添加要扫描的列
        Field[] fields = clz.getDeclaredFields();
        for (Field field : fields) {
            String fieldName = field.getName();
            scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(fieldName));
        }

        if (rowRange.length >= 1) {
            scan.setStartRow(Bytes.toBytes(rowRange[0]));
            scan.setStopRow(Bytes.toBytes(rowRange[1]));
        }

        // 读取指定表的全部数据
        return jhc.hbaseRDD(TableName.valueOf(originTable), scan);
    }

    /**
     * 批量写入到另一个表
     * @param jhc JavaHBaseContext
     * @param hBaseRdd JavaRDD<Tuple2<ImmutableBytesWritable, Result>>
     * @param destTable String
     * @param clz Class<?>
     * @param rowCondition String[]
     */
    public static void write(JavaHBaseContext jhc, JavaRDD<Tuple2<ImmutableBytesWritable, Result>> hBaseRdd, String destTable, Class<?> clz, String... rowCondition) {
        // 批量写入到另一个表
        jhc.bulkPut(hBaseRdd, TableName.valueOf(destTable), line -> {

            Object obj = HBaseUtil.result2Obj(line._2, clz);

            return HBaseUtil.obj2Put(obj, rowCondition);
        });
    }

    /**
     * 批量删除
     * @param jhc JavaHBaseContext
     * @param hBaseRDD JavaRDD<Tuple2<ImmutableBytesWritable, Result>>
     * @param originTable String
     */
    public static void delete(JavaHBaseContext jhc, JavaRDD<Tuple2<ImmutableBytesWritable, Result>> hBaseRDD, String originTable) {
        jhc.bulkDelete(hBaseRDD, TableName.valueOf(originTable), line -> new Delete(line._2.getRow()), 20);
    }

}
