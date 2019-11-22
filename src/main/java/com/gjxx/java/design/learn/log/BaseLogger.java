package com.gjxx.java.design.learn.log;

import java.util.logging.Level;

/**
 * @author Sxs
 * @description 记录日志的抽象类
 * @date 2019/11/21 9:58
 */
public abstract class BaseLogger {

    private String name;

    private boolean enabled;

    private Level minPermittedLevel;

    public BaseLogger(String name, boolean enabled, Level minPermittedLevel) {
        this.name = name;
        this.enabled = enabled;
        this.minPermittedLevel = minPermittedLevel;
    }

    public void log(Level level, String message) {
        boolean loggable = enabled && (minPermittedLevel.intValue() <= level.intValue());
        if (!loggable) {
            return;
        }
        doLog(level, message);
    }

    /**
     * 打印日志，由子类完成具体实现
     * @param level 日志等级
     * @param message 日志信息
     */
    public abstract void doLog(Level level, String message);

}
