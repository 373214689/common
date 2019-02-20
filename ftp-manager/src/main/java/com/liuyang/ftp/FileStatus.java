package com.liuyang.ftp;


import java.io.File;
import java.util.function.LongFunction;

/**
 * File Status
 *
 * @author liuyang
 * @version 1.0.0
 */
public interface FileStatus {
    /** 无 */
    public static final int STATUS_NONE = 0;
    /** 已接收 */
    public static final int STATUS_RECEIVED = 1;
    /** 已发送 */
    public static final int STATUS_SENT = 2;
    /** 中断 */
    public static final int STATUS_ABORT = 4;
    /** 不存在 */
    public static final int STATUS_NOT_EXISTS = 8;

    /**
     * 获取长度
     * @return 返回长度。这个长度是由 setLength 指定的，可以指代数据长度等等。
     */
    long getLength();

    /**
     * 获取本地文件
     * @return 返回一个文件
     */
    File getLocalFile();

    /**
     * 获取名称
     * @return 返回文件状态名称
     */
    String getName();

    /**
     * 获取状态
     * @return 返回状态信息，可能会取以下值之一：
     *         <ul>
     *             <li>STATUS_ABORT：-2，表示异常中断。</li>
     *             <li>STATUS_NOT_EXISTS：-1，表示文件不存在。</li>
     *             <li>STATUS_NONE：0，表示文件无状态（正常）</li>
     *             <li>STATUS_RECEIVED：1，表示文件已被接收（下载）。</li>
     *             <li>STATUS_SENT: 2, 表示文件已发送（上传）</li>
     *         </ul>
     */
    int getStatus();

    /**
     * 设定结束时间
     * @param time 指定时间。单位：ms
     */
    void setEndTime(long time);

    /**
     * 设置结束时间
     */
    default void setEndTime() {
        setEndTime(System.currentTimeMillis());
    }

    /**
     * 设置长度
     * @param length 指定长度
     */
    void setLength(long length);

    /**
     * 设置长度
     * @param action 指定长度计算表达式
     */
    default void setLength(LongFunction<Long> action) {
        setLength(action.apply(getLength()));
    }

    /**
     * 设置本地路径
     * @param local 指定本地路径
     */
    void setLocalFile(File local);

    /**
     * 设置名称
     * @param name 指定名称
     */
    void setName(String name);

    /**
     * 设定开始时间
     * @param time 指定时间。单位：ms
     */
    void setStartTime(long time);

    /**
     * 设置开始时间
     */
    default void setStartTime() {
        setStartTime(System.currentTimeMillis());
    }
    /**
     * 设定状态
     * @param status 指定状态。可能会取以下值之一：
     *               <ul>
     *                   <li>STATUS_ABORT：-2，表示异常中断。</li>
     *                   <li>STATUS_NOT_EXISTS：-1，表示文件不存在。</li>
     *                   <li>STATUS_NONE：0，表示文件无状态（正常）</li>
     *                   <li>STATUS_RECEIVED：1，表示文件已被接收。</li>
     *                   <li>STATUS_SENT: 2, 表示文件已发送（上传）</li>
     *               </ul>
     */
    void setStatus(int status);


}
