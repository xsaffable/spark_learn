package com.gjxx.java.design.learn.log;

import java.io.IOException;
import java.io.Writer;
import java.util.logging.Level;

/**
 * @author Sxs
 * @description 输出日志到文件
 * @date 2019/11/21 10:09
 */
public class FileLogger extends BaseLogger {

    private Writer fileWriter;

    public FileLogger(String name, boolean enabled, Level minPermittedLevel, Writer fileWriter) {
        super(name, enabled, minPermittedLevel);
        this.fileWriter = fileWriter;
    }

    /**
     * 格式化level和message,输出到日志文件
     * @param level 日志等级
     * @param message 日志信息
     */
    @Override
    public void doLog(Level level, String message) {
        try {
            this.fileWriter.write(message);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
