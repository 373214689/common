package com.liuyang.ds;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 数据记录接口
 * <ul>
 *     <li>2019/2/19 ver 1.0.0 LiuYang 创建 </li>
 *     <li>2019/2/20 ver 1.0.1 LiuYang 新增功能 <code>header, toList</code></li>
 * </ul>
 * @param <E>
 * @author liuyang
 * @version 1.0.1
 */
public interface DataRecord<E> extends Closeable {

    /**
     * 以数据流的形式输出数据
     * @return 返回数据流
     * @throws Exception 创建数据流的过程中遇到错误时抛出该异常。
     */
    Stream<E> stream() throws Exception;

    /**
     * 获取数据模型
     * @return 返回数据模型字段数组。
     */
    Schema[] header();

    /**
     * 以列表的形式输出数据
     * @return 返回数据列表
     * @throws Exception 创建数据列表的过程中遇到错误时抛出该异常。
     */
    default List<E> toList() throws Exception {
        return stream().collect(Collectors.toList());
    }
}
