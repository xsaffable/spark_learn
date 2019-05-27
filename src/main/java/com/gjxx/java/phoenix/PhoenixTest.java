package com.gjxx.java.phoenix;

/**
 * phoenix的测试类
 * @author Admin
 */
public class PhoenixTest {

    public static void main(String[] args) {

        PhoenixUtil.read("\"student\"");

        PhoenixUtil.close();
    }

}
