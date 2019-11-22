package com.gjxx.java.design.learn.log;

import java.util.logging.Level;

/**
 * @author Sxs
 * @description 输出日志到消息中间件
 * @date 2019/11/21 10:47
 */
public class MessageQueueLogger extends BaseLogger {

    public MessageQueueLogger(String name, boolean enabled, Level minPermittedLevel) {
        super(name, enabled, minPermittedLevel);
    }

    @Override
    public void doLog(Level level, String message) {

    }

}
