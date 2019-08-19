package com.gjxx.java.java.learn;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @ClassName Demo02
 * @Description TODO
 * @Author SXS
 * @Date 2019/8/16 17:29
 * @Version 1.0
 */
public class Demo02 {

    public static void main(String[] args) throws ParseException {
        String str = "20190814005914";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        long time = sdf.parse(str).getTime();
        System.out.println(time);
        System.out.println(sdf.parse(str));

        Date date = new Date(1565715554000L);
        String ss = sdf.format(date);
        System.out.println(ss);
    }

}
