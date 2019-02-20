package com.liuyang.jdbc;

import com.liuyang.ds.Parser;
import com.liuyang.ds.Schema;
import com.liuyang.ds.Type;

import java.io.Serializable;
import java.util.Properties;

public final class Column implements Schema, Serializable, Cloneable {

    /**
     * 解析
     * 从文本中解析 name, type, scale, precision 等数据
     * @param text
     */
    public static Column parse(String text) {
        String  name      = null;
        Type    type      = null;
        int     scale     = 0;
        int     precision = 0;
        boolean nullable  = true;
        boolean primary   = false;
        // 查找空格
        int space = text.indexOf(' ');
        if (space == -1)
            throw new IllegalArgumentException("can' t parse text: " + text + ", format is invalid.");
        name = text.substring(0, space).trim();
        type = null;
        // 查找左括号，没有找到则需要通过空格界定 type 的字段范围
        // 如果查找到左括号和右括号
        int lbracket = text.indexOf('(', space);
        if (lbracket == -1) {
            int range = text.indexOf(' ', space);
            // 界定 type 的字段范围
            range = range == -1 ? text.length() : range;
            type = Type.lookup(text.substring(space + 1, range).trim());
        } else {
            int rbracket = text.indexOf(')', lbracket);
            if (space == -1)
                throw new IllegalArgumentException("can' t parse text: " + text + ", format is invalid.");
            type = Type.lookup(text.substring(space + 1, lbracket).trim());
            int comma = text.indexOf(',', lbracket);
            if (comma == -1) {
                scale = Parser.parseInt(text.substring(lbracket + 1, rbracket).trim());
            } else {
                scale = Parser.parseInt(text.substring(lbracket + 1, comma).trim());
                precision = Parser.parseInt(text.substring(comma + 1, rbracket).trim());
            }
        }
        // 其他完整性检测
        if (text.toLowerCase().indexOf("NOT NULL") != -1) {
            nullable = false;
        }

        if (text.toLowerCase().indexOf("PRIMARY KEY") != -1) {
            primary  = true;
            nullable = false;
        }
        Column column = new Column(name, type, scale, precision);
        column.nullable = nullable;
        column.primary  = primary;
        return column;
    }

    /** 属性名称 */
    private String name;

    /** 属性类型： 规定了属性数据类型和取值范围 */
    private Type type;

    /** 精度：主要用于小数点精度确认，如double(11, 6) */
    private int precision = 0;

    /** 刻度：主要用于字长确认，如varchar(255), int(11)等形式 */
    private int scale = 0;

    /** 可空：确认属性值是否可空，默认可以为空 */
    private boolean nullable = true;

    private int index = 0;

    /** 记录name的hash值, 在hashCode方法中使用该值。 避免频繁调用String.hashCode方法。 */
    private int hash = 0;

    /** 主键 */
    private boolean primary = false;

    /** maxLength 必须大于 minLength。
     /** 最小长度，字段限定值，对于文本则是限定最小长度，数字则限定下限值，0 表示不限定 */
    private int minLength = 0;
    /** 最大长度，字段限定值，对于文本则是限定最大长度，数字则限定上限值，0 表示不限定 */
    private int maxLength = 0;
    /** 互斥属：假如同一个互斥属组中某一个元素满足，则其元素则不能被满足, 0 表示不作互斥判定 */
    private int mutexGroup = 0;
    /** 同属，假如同一个同属组中某一个元素满足，则表示其他的元素都被满足, 0 表示不作同属判定 */
    private int sameGroup = 0;

    private Properties prop;


    /**
     * 创建字段
     * @param name      字段名称
     * @param type      字段类型
     * @param scale     字段刻度
     * @param precision 字段精度
     */
    public Column(String name, Type type, int scale, int precision) {
        this.name      = name;
        this.type      = type;
        this.scale     = scale;
        this.precision = precision;
        this.hash      = name.hashCode();
    }

    public Column(String name, Type type, int scale, int precision, boolean primary) {
        this(name, type, scale, precision);
        setPrimary(primary);
    }

    /**
     * 创建字段
     * @param name      字段名称
     * @param typeName  字段类型名称
     * @param scale     字段刻度
     * @param precision 字段精度
     */
    public Column(String name, String typeName, int scale, int precision) {
        this(name, Type.lookup(typeName), scale, precision);
    }

    /**
     * 创建字段
     * @param name      字段名称
     * @param type      字段类型
     */
    public Column(String name, Type type) {
        this(name, type, 0, 0);
    }

    /**
     * 创建字段
     * @param name      字段名称
     * @param typeName  字段类型名称
     */
    public Column(String name, String typeName) {
        this(name, Type.lookup(typeName), 0, 0);
    }

    @Override
    protected void finalize() {
        this.name      = null;
        this.type      = null;
        this.scale     = 0;
        this.precision = 0;
        this.hash      = 0;
    }

    @Override
    public Column clone() {
        Column column = new Column(name, type, scale, precision);
        column.index = index;
        column.nullable = nullable;
        column.primary = primary;
        column.maxLength = maxLength;
        column.minLength = minLength;
        column.mutexGroup = mutexGroup;
        column.mutexGroup = sameGroup;
        return column;
    }

    @Override
    public boolean equals(Object anObject) {
        if (anObject == null) return false;
        if (anObject == this) return true;
        if (anObject instanceof Column) {
            Column other = (Column) anObject;
            return other.name.equals(name) && other.type.equals(type);
        }
        if (anObject instanceof Schema) {
            Schema other = (Schema) anObject;
            return other.getName().equals(name) && other.getType().equals(type);
        }
        return false;
    }

    @Override
    public int hashCode() {
        long result = type.ordinal() * 4241 + hash + precision * 13 + scale;
        return (int) result;
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public int getScale() {
        return scale;
    }

    @Override
    public int getPrecision() {
        return precision;
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

    @Override
    public boolean isPrimary() {
        return primary;
    }

    @Override
    public Object getProperty(String name) {
        return null;
    }

    /**
     * 获取互斥属性组字段
     * @return
     */
    public int getMutexGroup() {
        return mutexGroup;
    }

    /**
     * 获取相同属性组字段
     * @return
     */
    public int getSameGroup() {
        return sameGroup;
    }

    /**
     * 获取字段最大长度
     * @return
     */
    public int getMaxLength() {
        return maxLength;
    }

    /**
     * 获取字段最小长度
     * @return
     */
    public int getMinLength() {
        return minLength;
    }

    public void setMaxLength(int length) {
        this.maxLength = length;
    }

    public void setMinLength(int length) {
        this.minLength = length;
    }

    public void setMutexGroup(int group) {
        this.mutexGroup = group;
    }

    public void setSameGroup(int group) {
        this.sameGroup = group;
    }

    @Override
    public void setIndex(int index) {
        this.index = index;
    }

    public synchronized void setName(String name) {
        this.name = name;
        this.hash = name.hashCode();
    }

    public synchronized void setType(Type type) {
        this.type = type;
    }

    public synchronized void setType(String typeName) {
        this.type = Type.lookup(typeName);
    }

    public synchronized void setScale(int scale) {
        this.scale = scale;
    }

    public synchronized void setPrecision(int precision) {
        this.precision = precision;
    }

    public synchronized void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public synchronized void setPrimary(boolean isPrimary) {
        this.primary = isPrimary;
        // 设置为主键后，可空属性为false;
        if (isPrimary) this.nullable = false;
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(name).append(' ').append(type.getName());
        switch(type) {
            case DOUBLE: {
                if (precision > 0)
                    buffer.append('(').append(scale == 0 ? 11 : scale).append(", ").append(precision).append(')');
                break;
            }
            case FLOAT: {
                if (precision > 0)
                    buffer.append('(').append(scale == 0 ? 11 : scale).append(", ").append(precision).append(')');
                break;
            }
            case INT:
            case BIGINT:
            case SMALLINT:
            case VARCHAR: {
                if (scale > 0)
                    buffer.append('(').append(scale).append(')');
                break;
            }
            default:
                break;
        }
        if (primary == true)
            buffer.append(" primary key");

        if (nullable == false)
            buffer.append(" not null");

        return buffer.toString();
    }
}
