package com.liuyang.ds;

/**
 * Record Interface
 * @param <K>
 * @param <V>
 *
 * @author liuyang
 * @version 1.0.0
 */
public interface Record<K, V> {
    /**
     * 获取标识
     * @return 返回标识。
     */
    long getId();

    /**
     * 获取键
     * @return 返回键
     */
    K getKey();

    /**
     * 获取值
     * @return 返回值
     */
    V getValue();

    /**
     * 获取时间戳
     * @return 返回时间戳。单位：毫秒
     */
    long getTimestamp();

    /**
     * 获取记录长度
     * @return 返回记录长度。
     */
    long getLength();


}
