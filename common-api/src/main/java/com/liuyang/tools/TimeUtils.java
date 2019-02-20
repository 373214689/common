package com.liuyang.tools;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public final class TimeUtils {
    private final static long PROCESS_START_TIME = System.currentTimeMillis();

    /** 时区偏置毫秒数 */
    public final static long TIMEZONE_OFFSET_MILLIS = 28800000L;
    /** 一天时间毫秒数 */
    public final static long DAY_MILLIS = 86400000L;
    /** 一小时时间毫秒数 */
    public final static long HOUR_MILLIS = 3600000L;
    /** 一分钟时间毫秒数 */
    public final static long MINUTE_MILLIS = 60000;

    public final static long SECOND_MILLIS = 1000;

    private final static String DATE_FORMATTER_0 = "yyyy-MM-dd HH:mm:ss";
    private final static String DATE_FORMATTER_2 = "yyyyMMddHHmmss";


    private static ThreadLocal<Map<String, SimpleDateFormat>>
            DATE_FORMATTER_MAP = ThreadLocal.withInitial(HashMap::new);

    private static ThreadLocal<Map<String, Long>>
            RUNTIME_TIMER_MAP = ThreadLocal.withInitial(HashMap::new);


    /**
     * 返回一个SimpleDateFormat,每个线程只会new一次pattern对应的sdf
     *
     * @param pattern Formatter 表达式
     * @return 返回日期时间格式化工具实例。
     */
    private static SimpleDateFormat restoreFormatter(final String pattern) {
        Map<String, SimpleDateFormat> map = DATE_FORMATTER_MAP.get();
        SimpleDateFormat formatter = map.get(pattern);
        if (formatter == null) {
            formatter = new SimpleDateFormat(pattern);
            map.put(pattern, formatter);
        }
        return formatter;
    }

    /*private static Long restoreTimer(final long time) {
        Map<String, Long> map = RUNTIME_TIMER_MAP.get();
        Long formatter = map.get("Runtime");
        if (formatter == null) {
            formatter = new SimpleDateFormat(pattern);
            map.put(pattern, formatter);
        }
        return formatter;
    }*/

    /**
     * 格式化时间
     *
     * @param date 指定日期时间
     * @return 返回格式化后的数据。
     */
    public static String format(Date date) {
        return restoreFormatter(DATE_FORMATTER_0).format(date);
    }

    public static String format(long millis) {
        return format(new Date(millis));
    }

    public static String formatWithoutSymbol(Date date) {
        return restoreFormatter(DATE_FORMATTER_2).format(date);
    }

    public static String formatWithoutSymbol(long millis) {
        return formatWithoutSymbol(new Date(millis));
    }

    public static String format(String formatter, Date date) {
        return restoreFormatter(formatter).format(date);
    }

    public static String format(String formatter, long millis) {
        return format(formatter, new Date(millis));
    }



    /**
     * 获取日期戳
     * @param timestamp 指定时间戳。单位：毫秒。
     * @return 反思回日期数值
     */
    public static int getDatestamp(long timestamp) {
        long time = timestamp <= 0 ? System.currentTimeMillis() : timestamp;
        time += TIMEZONE_OFFSET_MILLIS;
        return (int) ((time - time % DAY_MILLIS) / DAY_MILLIS);
    }


    public static int getDatestamp(Date date) {
        return getDatestamp(date.getTime());
    }

    public static int getDatestamp() {
        return getDatestamp(System.currentTimeMillis());
    }

    public static int getDay(long timestamp) {
        long time = timestamp <= 0 ? System.currentTimeMillis() : timestamp;
        time += TIMEZONE_OFFSET_MILLIS;
        return (int) ((time - time % DAY_MILLIS) / DAY_MILLIS);
    }

    public static int getHour(long timestamp) {
        long time = timestamp <= 0 ? System.currentTimeMillis() : timestamp;
        time += TIMEZONE_OFFSET_MILLIS;
        return (int) ((time % DAY_MILLIS - time % HOUR_MILLIS) / HOUR_MILLIS);
    }

    public static int getMinute(long timestamp) {
        long time = timestamp <= 0 ? System.currentTimeMillis() : timestamp;
        time += TIMEZONE_OFFSET_MILLIS;
        return (int) ((time % HOUR_MILLIS - time % MINUTE_MILLIS) / MINUTE_MILLIS);
    }

    public static int getSecond(long timestamp) {
        long time = timestamp <= 0 ? System.currentTimeMillis() : timestamp;
        time += TIMEZONE_OFFSET_MILLIS;
        return (int) ((time % MINUTE_MILLIS - time % SECOND_MILLIS) / SECOND_MILLIS);
    }


    /**
     * 获取时间差
     * @param nanos 纳秒时间戳，使用System.nanoTime()获取。
     * @return 返回时间差，单位：秒。
     */
    public static double getUsedTimeNS(long nanos) {
        return ((double) (System.nanoTime() - nanos)) / 1000000 / 1000;
    }

    /**
     * 获取时间差
     * @param start 开始时间毫秒时间戳。
     * @param end   结束时间毫秒时间戳。
     * @return 返回时间差，单位：秒。
     */
    public static double getUsedTimeMS(long start, long end) {
        return ((double) (end - start)) / 1000;
    }

    /**
     * 获取时间差
     * <p>
     *     在此之前，需要调用 <code>startTimer</code> 和 <code>stopTimer</code> 然后才能获取两者间的时间差。
     *     该功能独属于线程，不同线程之间不会共享同一个时间。
     * </p>
     * @return 返回时间差，单位：秒。
     */
    public static double getUsedTimeMS() {
        Map<String, Long> map = RUNTIME_TIMER_MAP.get();
        long start = map.get("RuntimeStart");
        long end   = map.get("RuntimeStop");
        return ((double) (end - start)) / 1000;
    }

    /**
     * 获取时间差
     * @param millis 毫秒时间戳，使用System.currentTimeMillis()获取。
     * @return 返回时间差，单位：秒。
     */
    public static double getUsedTimeMS(long millis) {
        return ((double) (System.currentTimeMillis() - millis)) / 1000;
    }

    public static long getRunTime() {
        return System.currentTimeMillis() - PROCESS_START_TIME;
    }

    /**
     * Get time of now with a formatter.
     * @return formatted by "yyyy-MM-dd HH:mm:ss.ms"
     */
    public static String now() {
        Date date = new Date();
        return String.format("%s.%03d", format(date), date.getTime() % 1000);
    }

    // 解析时间。数据中除了带有时间数字外，还带有分隔标识。
    // 如：2019-01-01 12:30:59 （2019年1月1日12点30分59秒）
    public static Date parse(String source) {
        try {
            return restoreFormatter(DATE_FORMATTER_0).parse(source);
        } catch (ParseException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("Parse date(" + source + ") as [" + DATE_FORMATTER_0 + "] failure.", e);
        }
    }

    // 解析时间。数据中只有时间数字，没有其他多余的符号。
    // 如：20190101123059 （2019-01-01 12:30:59, 2019年1月1日12点30分59秒）
    public static Date parseWithoutSymbol(String source) {
        try {
            return restoreFormatter(DATE_FORMATTER_2).parse(source);
        } catch (ParseException e) {
            //e.printStackTrace();
            throw new IllegalArgumentException("Parse date(" + source + ") as [" + DATE_FORMATTER_2 + "] failure.", e);
        }
    }

    public static Date parse(String formatter, String source) {
        try {
            return restoreFormatter(formatter).parse(source);
        } catch (ParseException e) {
            //e.printStackTrace();
            throw new IllegalArgumentException("Parse date(" + source + ") as [" + formatter + "] failure.", e);
        }
    }

    /**
     * Get time of now with a formatter.
     * @return formatted by "yyyy-MM-dd HH:mm:ss.ms"
     */
    public static String showTime(long currentTimeMillis) {
        Date date = currentTimeMillis <= 0 ? new Date() : new Date(currentTimeMillis);
        return String.format("%s.%03d", format(date), date.getTime() % 1000);
    }

    public static String showTime() {
        Date date = new Date();
        return String.format("%s.%03d", format(new Date()), date.getTime() % 1000);
    }

    /**
     * 开始线程计时。
     * <p>
     *     需要与 <code>stopTimer</code> 配合使用，用于计算两者之间的时间差。
     * </p>
     * @return 返回时间戳。单位：毫秒。
     */
    public static long startTimer() {
        Map<String, Long> map = RUNTIME_TIMER_MAP.get();
        long current = System.currentTimeMillis();
        map.put("RuntimeTimerStart", current);
        map.put("RuntimeTimerStop", current);
        return current;
    }

    /**
     * 停止线程计时。
     * <p>
     *     需要与 <code>startTimer</code> 配合使用，用于计算两者之间的时间差。
     * </p>
     * @return 返回时间戳。单位：毫秒。
     */
    public static long stopTimer() {
        Map<String, Long> map = RUNTIME_TIMER_MAP.get();
        long current = System.currentTimeMillis();
        map.put("RuntimeTimerStop", current);
        return current;
    }

    /**
     * 线程休眠
     * @param millis 指定毫秒
     */
    public static void trySleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static Date tomorrow(Date date) {
        if (date == null)
            throw new NullPointerException("parmameter date does not specified.");
        return new Date(date.getTime() + DAY_MILLIS);
    }

    public static Date tomorrow() {
        return new Date(System.currentTimeMillis() + DAY_MILLIS);
    }

    public static Date yestoday(Date date) {
        if (date == null)
            throw new NullPointerException("parmameter date does not specified.");
        return new Date(date.getTime() - DAY_MILLIS);
    }

    public static Date yestoday() {
        return new Date(System.currentTimeMillis() - DAY_MILLIS);
    }
}
