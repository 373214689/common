package com.liuyang.csv;

import com.liuyang.ds.Row;

/**
 * CSV 数据记录
 * <ul>
 *     <li>2019/1/22 ver 1.0.0 创建。</li>
 * </ul>
 * @author liuyang
 * @version 1.0.0
 */
public interface CSVRecord {

    /**
     * 获取索引
     * @return 返回索引号（一般是行号）。
     */
    long getIndex();

    /**
     * 获取数据字节长度
     * @return 返回数据字节长度。
     */
    int getBytes();

    /**
     * 获取文件
     * @return 返回对应的原始文本。
     */
    String getText();

    /**
     * 获取行数据
     * @return 返回对应的已被解析的行数据。
     */
    Row getRow();
}
