package com.gjxx.java.java.learn.annotations;

import java.lang.annotation.*;

/**
 * @author Sxs
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface FruitColor {

    /**
     * 颜色枚举
     */
    public enum Color{
        /**
         * 颜色
         */
        BLUE, RED, GREEN
    }

    Color fruitColor() default Color.BLUE;

}
