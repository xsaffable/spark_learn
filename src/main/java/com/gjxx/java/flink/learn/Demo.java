package com.gjxx.java.flink.learn;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName Demo
 * @Description flink学习
 * @Author SXS
 * @Date 2019/8/7 14:24
 * @Version 1.0
 */
public class Demo {

    public static void main(String[] args) {
        // 创建flink运行环境上下文
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取文件
        DataStreamSource<String> dss = env.readTextFile("file/borrowInfo.log");

        dss.print();

    }

}
