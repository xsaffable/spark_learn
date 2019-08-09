package com.gjxx.java.java.learn;

import com.gjxx.java.java.learn.annotations.FruitColor;
import com.gjxx.java.java.learn.annotations.FruitName;
import lombok.Data;

/**
 * @ClassName Apple
 * @Description 苹果类
 * @Author Sxs
 * @Date 2019/8/9 10:11
 * @Version 1.0
 */
@Data
public class Apple {

    @FruitName("Apple")
    private String appleName;

    @FruitColor(fruitColor = FruitColor.Color.RED)
    private String appleColor;



}
