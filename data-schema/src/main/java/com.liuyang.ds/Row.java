package com.liuyang.ds;

import java.util.Collection;
import java.util.Map;

/**
 * 行 接口
 * <p>
 *     <i>****** 接口功能存在冗余，待重构 ******</i>
 * </p>
 * <ul>
 *     <li>2019/1/4   ver 1.0.0 创建。</li>
 *     <li>2019/1/21  ver 1.0.1 添加功能。 <code>setValue</code>.</li>
 *     <li>2019/1/23  ver 1.0.2 添加功能。 <code>collect</code>.</li>
 *     <li>2019/2/19  ver 1.0.3 添加功能。 <code>get, toString, toArray</code>.</li>
 * </ul>
 * @author liuyang
 * @version 1.0.3
 */
public interface Row {

    /**
     * 选取所有数据。
     * @return 返回 <code>Object</code> 数据集合。
     */
    Collection<Object> collect();

    /**
     * 选取数据
     * @param startIndex 开始索引
     * @param num 选取的数据个数。取值：小等于 0 或超出数据选取范围时， 选取所有数据； 大于 1 时，选取指定数量数据。
     * @return 返回 <code>Object</code> 数据集合。
     */
    Collection<Object> collect(int startIndex, int num);

    /**
     * 选取数据
     * @param fieldNames 指定字段
     * @return 返回 <code>Object</code> 数据集合。
     */
    Collection<Object> collect(String... fieldNames);

    /**
     * 选取数据
     * @param primary 取值：true 表示只获取主键数据；false 表示只获取非主键数据。
     * @return 返回 <code>Object</code> 数据集合。
     */
    Collection<Object> collect(boolean primary);

    /**
     * 取回  <code>Boolean</code>  值
     * @param index 索引
     * @return 返回指定索引位置的 <code>Boolean</code> 值
     */
    boolean getBoolean(int index);

    /**
     * 取回  <code>Boolean</code>  值
     * @param field 字段
     * @return 返回指定字段名称的 <code>Boolean</code> 值
     */
    boolean getBoolean(String field);

    /**
     * 取回字节数组
     * @param index 索引
     * @return 返回指定索引位置的字节数组
     */
    byte[] getBinary(int index);

    /**
     * 取回字节数组
     * @param field 字段
     * @return 返回指定字段名称的字节数组
     */
    byte[] getBinary(String field);

    double getDouble(int index);

    double getDouble(String field);

    float getFloat(int index);

    float getFloat(String field);

    int getInteger(int index);

    int getInteger(String field);

    long getLong(int index);

    long getLong(String field);

    short getShort(int index);

    short getShort(String field);

    String getString(int index);

    String getString(String field);

    Object get(int index);

    Object get(String field);

    Value getValue(int index);

    Value getValue(String field);


    /**
     * 获取表头。此项不区分主键和非主键。
     * @return 返回 <code>Schema</code> 数组
     */
    Schema[] header();

    /**
     *  获取表头
     * @param primary 取值：true 表示只获取主键表头；false 表示只获取非主键表头。
     * @return 返回 <code>Schema</code> 数组
     */
    Schema[] header(boolean primary);

    /**
     * 将数据以数据形式输出。
     * @return 返回数据数组。
     */
    Object[] toArray();

    /**
     * 转换为 JSON 格式数据。
     * @return 返回 JSON 格式数据。
     */
    String toJSON();

    /**
     * 转换为 Map 数据
     * @return 返回 Map 类型数据： <字段名, 数据>。
     */
    Map<Schema, Object> toMap();


    /**
     * 转换为 Map 数据
     * @param startIndex 超始位置，不能小于 0。
     * @param num 数据数量，不能超出数据最大个数 <code>Row.size()<code/>。
     * @return 返回指定位置所对应的 Map 类型数据： <字段名, 数据>。
     */
    Map<Schema, Object> toMap(int startIndex, int num);

    /**
     * 转换为 Map 数据
     * @param fieldNames 指定字段名。可以指定一个或多个，用“，”分隔，各个字段不可重复。
     *                   如果为单个“*”，则会输入所有字段。如果指定的字段不存在于时，则会抛出异常。
     * @return 返回指定字段名所对应的 Map 类型数据： <字段名, 数据>。
     */
    Map<Schema, Object> toMap(String... fieldNames);

    /**
     * 转换为 Map 数据
     * @param primary 取值：true 表示只获取主键数据；false 表示只获取非主键数据。
     * @return 返回对应的 Map 类型数据： <字段名, 数据>。
     */
    Map<Schema, Object> toMap(boolean primary);

    Map<String, Object> toNamedMap();

    Object setValue(int index, Object value);

    Object setValue(String field, Object value);

    /**
     * 连接所有元素，做为文本输出。
     * @param delimiter 分隔符
     * @return 返回连接后的文本
     */
    String toString(char delimiter);

    /**
     * 连接所有元素，做为文本输出。
     * @param delimiter 分隔符
     * @return 返回连接后的文本
     */
    String toString(String delimiter);


    /**
     * 从指定的位置分割出新的数据
     * @param startIndex 超始位置，不能小于 0。
     * @param endIndex 结束位置，不能超出数据最大个数 <code>Row.size()<code/>。
     * @return 返回新的 <code>Row</code> 数据
     */
    Row split(int startIndex, int endIndex);

    /**
     * 抽取指定字段的数据形成新的 <code>Row</code> 数据
     * @param fieldNames 指定字段名。可以指定一个或多个，用“，”分隔，各个字段不可重复。
     *                   如果为单个“*”，则会输入所有字段。如果指定的字段不存在于时，则会抛出异常。
     * @return 返回新的 <code>Row</code> 数据
     */
    Row split(String... fieldNames);


    /**
     * 获取数据
     * @return 返回 <code>Value</code> 数组
     */
    Value[] values();

    /**
     * 获取数据
     * @param primary 取值：true 表示只获取主键数据；false 表示只获取非主键数据。
     * @return 返回 <code>Value</code> 数组
     */
    Value[] values(boolean primary);

}
