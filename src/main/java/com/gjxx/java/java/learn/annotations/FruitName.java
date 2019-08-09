package com.gjxx.java.java.learn.annotations;

import java.lang.annotation.*;

/**
 * 注解练习
 * @author Sxs
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface FruitName {

    String value() default "";

}
