package com.liuyang.log;

import java.io.IOException;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Logger Class
 * <li>2018/06/28 created by liuyang.</li>
 * @author liuyang
 * @version 1.0.0
 *
 */
public class Logger {
    public static boolean ENABLE_LOGGER = true;
    public static boolean ENABLE_WRITER = true;
    public static boolean ENABLE_DEBUG = true;
    public static boolean ENABLE_INFO = true;
    public static boolean ENABLE_ERROR = true;
    public static boolean ENABLE_WARN = true;

    public synchronized static Logger getLogger(Class<?> clazz) {
        return new Logger(clazz, null);
    }

    /**
     * Get time of now with a formatter.
     * @return formatted by "yyyy-MM-dd HH:mm:ss.ms"
     */
    private static String now() {
        Date date = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss");
        return String.format("%s.%03d", formatter.format(date), date.getTime() % 1000);
    }

    private Class<?> clazz;
    private String className;
    private Writer writer;
    private boolean enableWriter = ENABLE_WRITER;
    private boolean enableLogger = ENABLE_LOGGER;

    /**
     * Loger
     * @param clazz 指定类
     * @param writer write the log to the specified writer.
     */
    private Logger(Class<?> clazz, Writer writer) {
        this.clazz = clazz;
        this.writer = writer;
        this.className = this.clazz.getSimpleName();
    }

    @Override
    protected void finalize() {
        clazz = null;
        writer = null;
        className = null;
        enableWriter = false;
        enableLogger = false;
    }

    private synchronized String createMessage(String level, Object m) {
        String message = String.format("[%s] %s %s %s", now(), level, className,  m);
        if (enableLogger && enableWriter && writer != null) {
            try {
                writer.write(message + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return message;
    }

    private synchronized String createMessage(String level, String format, Object... m) {
        String message = String.format("[%s] %s %s %s", now(), level, className, String.format(format, m));
        if (enableLogger && enableWriter && writer != null) {
            try {
                writer.write(message + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return message;
    }

    public synchronized final void enableLogger(boolean flag) {
        enableLogger = flag;
    }

    public synchronized final void enableWriter(boolean flag) {
        enableWriter = flag;
    }

    public synchronized final void debug(Object m) {
        if (ENABLE_LOGGER && ENABLE_DEBUG && enableLogger)
            System.out.println(createMessage("DEBUG", m));
    }

    public synchronized final void debug(String format, Object... m) {
        if (ENABLE_LOGGER && ENABLE_DEBUG && enableLogger)
            System.out.println(createMessage("DEBUG", format, m));
    }

    public synchronized final void error(Object m) {
        if (ENABLE_LOGGER && ENABLE_ERROR && enableLogger)
            System.err.println(createMessage("ERROR", m));
    }


    public synchronized final void error(String format, Object... m) {
        if (ENABLE_LOGGER && ENABLE_ERROR && enableLogger)
            System.err.println(createMessage("ERROR", format, m));
    }

    public synchronized final void info(Object m) {
        if (ENABLE_LOGGER && ENABLE_INFO && enableLogger)
            System.out.println(createMessage("INFO", m));
    }

    public final void info(String format, Object... m) {
        if (ENABLE_LOGGER && ENABLE_INFO && enableLogger)
            System.out.println(createMessage("INFO", format, m));
    }

    public synchronized final void warn(Object m) {
        if (ENABLE_LOGGER && ENABLE_WARN && enableLogger)
            System.err.println(createMessage("WARN", m));
    }

    public synchronized final void warn(String format, Object... m) {
        if (ENABLE_LOGGER && ENABLE_WARN && enableLogger)
            System.err.println(createMessage("WARN", format, m));
    }
}
