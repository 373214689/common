package com.liuyang.ds;

public interface Schema {

    /**
     * 复制数据
     * <p>
     *     如果实现了 <code>Schema</code>，同时还可以选择实现 <code>Cloneable</code>。
     *     在数组复制时，会调用 <code>Cloneable</code>，以实现深层复制。
     * </p>
     * @return 返回复制的 <code>Schema</code> 数据。
     */
    Schema clone();

    /**
     * 索引
     * @return 获取索引，不设置是传回 0。
     */
    int getIndex();

    /**
     * 名称
     * @return 获取名称
     */
    String getName();

    /**
     * 类型
     * @return 获取精度，一般只在 double, float 等数类型时才会配置，一般情况下为 0。
     */
    int getPrecision();

    /**
     * 刻度
     * @return 获取刻度。如varchar(32), int (11)，此项根据数据库的不同而有不同的取值。
     */
    int getScale();

    /**
     * 精度
     * @return 获取类型。
     */
    Type getType();

    /**
     * 获取属性
     * @param name 属性名称
     * @return 取回 name 所对应的属性值，没有则传回 null。
     */
    Object getProperty(String name);

    /**
     * 是否可空
     * @return 返回 true 表示可空，false 表示不可为空。
     */
    boolean isNullable();

    /**
     * 是否主键
     * @return 返回 true 表示为主键， false 表示为非主键
     */
    boolean isPrimary();

    /**
     * 设置索引
     * @param index 指定索引值
     */
    void setIndex(int index);

}
