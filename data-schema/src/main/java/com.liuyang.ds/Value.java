package com.liuyang.ds;

/**
 * Value 接口
 * <p>
 *     主要是为了调节各种类型数据之间有互相转换，解决 Java 中的数据类型问题。
 * </p>
 * @author liuyang
 * @version 1.0.0
 */
public interface Value {

    /**
     * 获取 boolean 数据。
     * @return 返回 boolean 类型数据。
     */
    boolean getBoolean();

    /**
     * 获取 byte[] 数据
     * @return 返回 byte[] 类型数据
     */
    byte[] getBinary();

    /**
     * 获取 double 数据
     * @return 返回 double 类型数据。
     */
    double getDouble();

    /**
     * 获取 float 数据。
     * @return 返回 float 类型数据。
     */
    float getFloat();

    /**
     * 获取 int 数据
     * @return 返回 int 类型数据。
     */
    int getInteger();

    /**
     * 获取 int 数据
     * @return 返回 int 类型数据。
     */
    default int getInt() {
        return getInteger();
    }

    /**
     * 获取 long 数据
     * @return 返回 long 类型数据。
     */
    long getLong();

    /**
     * 获取 short 数据
     * @return 返回 short 类型数据。
     */
    short getShort();

    /**
     * 获取 String 数据
     * @return 返回 String 类型数据。
     */
    String getString();

    /**
     * 返回 Object 数据。
     * @return 返回 Object 类型数据。该数据指向的是 Value 所对应的数据本身的包装和引用。
     */
    Object getValue();

    /**
     * 设置 boolean
     * @param value boolean 数据。
     */
    void setValue(boolean value);

    /**
     * 设置 byte[] 值
     * @param value byte[] 数据。
     */
    void setValue(byte[] value);

    void setValue(double value);

    void setValue(float value);

    void setValue(int value);

    void setValue(long value);

    void setValue(short value);

    void setValue(String value);

    void setValue(Object value);

}
