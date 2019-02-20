package com.liuyang.ds.sets;

import com.liuyang.ds.*;
import com.liuyang.tools.StringUtils;
import com.sun.istack.internal.NotNull;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * 数据行
 * <ul>
 *     <li>2019/1/4   ver 1.0.0 创建。</li>
 *     <li>2019/1/21  ver 1.0.1 新增功能 <code>setValue</code> 。</li>
 *     <li>2019/1/22  ver 1.0.1 新增功能 <code>getModifiedCount</code> （此属于初始版本设计功能）。</li>
 *     <li>2019/2/19  ver 1.0.3 新增功能 <code>get, toString, toArray</code> （此属于初始版本设计功能）。</li>
 * </ul>
 * @author liuyang
 * @version 1.0.3
 * @see com.liuyang.ds.Row
 */
public final class DataRow implements Row, Comparable<Row>, AutoCloseable  {

    private volatile Schema[] header;
    private volatile Object[] values;

    private transient int size;
    private transient int modCount;

    public DataRow(Schema[] header) {
        this.header = header;
        this.size   = header.length;
        this.values = new Object[size];
    }

    public DataRow(Schema[] header, Object [] arr) {
        this.header = header;
        this.size = header.length;
        this.values = new Object[size];
        parse(arr);
    }

    /**
     * 查询字符串并返回其索引
     * @param name 字段名称
     * @return 返回字段名称对应的索引位置
     * @throws IndexOutOfBoundsException 无法匹配字段名称时抛出异常
     */
    private int find(String name) {
        Objects.requireNonNull(name);
        for (int i = 0; i < size; i++) {
            String fieldName = header[i].getName();
            //System.out.println(name + " ?? " + fieldName + " >> " + name.equals(fieldName));
            if (name.equals(fieldName)) return i;
        }
        throw new IndexOutOfBoundsException("can not found the index of [name = " + name + "].");
    }

    /**
     * Checks if the given index is in range.  If not, throws an appropriate
     * runtime exception.  This method does *not* check if the index is
     * negative: It is always used immediately prior to an array access,
     * which throws an ArrayIndexOutOfBoundsException if index is negative.
     *
     * @param index 索引
     * @throws IndexOutOfBoundsException 超出检索范围时抛出异常。
     */
    private void rangeCheck(int index) {
        if (index >= size || index < 0)
            throw new IndexOutOfBoundsException("index out of range [index = " + index + ", size: " + size + "]");
    }

    @Override
    public final void close() {
        header   = null;
        values   = null;
        size     = 0;
        modCount = 0;
    }

    @Override
    public final int compareTo(Row other) {
        if (other == null)
            return 1;
        if (other == this)
            return 0;
        return hashCode() - other.hashCode();
    }

    @Override
    public final boolean equals(Object o) {
        if (o == null)
            return false;
        if (o == this)
            return true;
        if (o instanceof DataRow) {
            DataRow other = (DataRow) o;
            return Arrays.equals(other.header, header) && Arrays.equals(other.values, values);
        }
        return false;
    }

    /**
     * 解析字符串数组
     * <p>
     *     如果字符串数组的长度不足以覆盖所有的原数据，
     * </p>
     * @param arr 字符串数组
     * @param start 从指定的起始位置覆盖原数据
     * @param truncate 是否清除不能覆盖的数据
     * @return 返回实例指向
     */
    public final DataRow parse(Object[] arr, int start, boolean truncate) {
        rangeCheck(start);
        modCount++;
        // 记录复制长度，数组长度不能超过 size。
        int length = arr.length <= size ? arr.length : size;
        // 复制数组数据， 原始数据指定位置是 start，总共会复制 length - start 个数据。
        // 复制后的数据截止下标不会超过 length。
        System.arraycopy(arr, start, values, 0, length - start);
        // 检测是否要清空未覆盖到的数据
        if (truncate)
            // 如果传入的数组长度未超过 size，则手动清空 values 中下标超过 length 的数据。
            for (int i = length; i < size; i++) {
                values[i] = "";
            }
        return this;
    }

    public final DataRow parse(Object[] arr) {
        return parse(arr, 0, true);
    }

    public final DataRow parse(String line, String regex) {
        return parse(line.split(regex));
    }

    @Override
    public final Collection<Object> collect() {
        return Arrays.stream(values, 0, size).collect(Collectors.toList());
    }

    @Override
    public final Collection<Object> collect(int startIndex, int num) {
        rangeCheck(startIndex);
        rangeCheck(startIndex + num);
        return Arrays.stream(values, startIndex, startIndex + num).collect(Collectors.toList());
    }

    @Override
    public final Collection<Object> collect(String... fieldNames) {
        if (fieldNames.length == 0)
            return collect();
        if ("*".equals(fieldNames[0]))
            return collect();
        for (String name : fieldNames) {
            if (find(name) < 0)
                throw new IllegalArgumentException(
                        "header has not contains field as ([" + name + "]).");
        }
        return IntStream.range(0, size).filter(i -> find(fieldNames[i]) != -1)
                .mapToObj(i -> values[i]).collect(Collectors.toList());
    }

    @Override
    public final Collection<Object> collect(boolean primary) {
        return IntStream.range(0, size).filter(i -> header[i].isPrimary() == primary)
                .mapToObj(i -> values[i]).collect(Collectors.toList());
    }

    @Override
    public final Object get(int index) {
        rangeCheck(index);
        return values[index];
    }

    @Override
    public final Object get(String fieldName) {
        return get(find(fieldName));
    }

    @Override
    public final byte[] getBinary(int index) {
        rangeCheck(index);
        return Parser.parseBinary(values[index]);
    }

    @Override
    public final byte[] getBinary(String fieldName) {
        return getBinary(find(fieldName));
    }

    @Override
    public final boolean getBoolean(int index) {
        rangeCheck(index);
        return Parser.parseBoolean(values[index]);
    }

    @Override
    public final boolean getBoolean(String fieldName) {
        return getBoolean(find(fieldName));
    }

    @Override
    public final double getDouble(int index) {
        rangeCheck(index);
        return Parser.parseDouble(values[index]);
    }

    @Override
    public final double getDouble(String fieldName) {
        return getDouble(find(fieldName));
    }

    @Override
    public final float getFloat(int index) {
        rangeCheck(index);
        return Parser.parseFloat(values[index]);
    }

    @Override
    public final float getFloat(String fieldName) {
        return getFloat(find(fieldName));
    }

    @Override
    public final int getInteger(int index) {
        rangeCheck(index);
        return Parser.parseInt(values[index]);
    }

    @Override
    public final int getInteger(String fieldName) {
        return getInteger(find(fieldName));
    }

    @Override
    public final long getLong(int index) {
        rangeCheck(index);
        return Parser.parseLong(values[index]);
    }

    @Override
    public final long getLong(String fieldName) {
        return getLong(find(fieldName));
    }

    @Override
    public final short getShort(int index) {
        rangeCheck(index);
        return Parser.parseShort(values[index]);
    }

    @Override
    public final short getShort(String fieldName) {
        return getShort(find(fieldName));
    }

    @Override
    public final String getString(int index) {
        rangeCheck(index);
        return String.valueOf(values[index]);
    }

    @Override
    public final String getString(String fieldName) {
        return getString(find(fieldName));
    }

    @Override
    public final Value getValue(int index) {
        rangeCheck(index);
        return Parser.parseValue(header[index].getType(), values[index]);
    }

    @Override
    public final Value getValue(String fieldName) {
        return getValue(find(fieldName));
    }


    @Override
    public final Schema[] header() {
        return Arrays.copyOf(header, header.length);
        //return header.clone();
    }

    @Override
    public final Schema[] header(boolean primary) {
        return Arrays.stream(header).filter(e -> e.isPrimary() == primary).toArray(Schema[]::new);
    }

    /**
     * 将数据输出为字符串
     * @param delimiter 字段分隔符号
     * @param containsHeader 是否输入标题
     * @return 返回字符串数据。
     */
    public final String join(char delimiter, boolean containsHeader) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < size; i++) {
            if (i > 0)
                builder.append(delimiter);
            if (containsHeader)
                builder.append(header[i].getName()).append('=');
            builder.append(values[i]);

        }
        return builder.toString();
    }

    /**
     * 将数据输出为字符串
     * @param delimiter 字段分隔符号
     * @param containsHeader 是否输入标题
     * @return 返回字符串数据。
     */
    public final String join(CharSequence delimiter, boolean containsHeader) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < size; i++) {
            if (i > 0)
                builder.append(delimiter);
            if (containsHeader)
                builder.append(header[i].getName()).append('=');
            builder.append(values[i]);

        }
        return builder.toString();
    }

    @Override
    public Object[] toArray() {
        return IntStream.range(0, size)
                .mapToObj(i -> Parser.parseValue(header[i], values[i]).getValue()).toArray(Object[]::new);
    }

    @Override
    public String toJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append('{');
        String[] arr = IntStream.range(0, size).mapToObj(i -> {
            StringBuilder data = new StringBuilder();
            data.append('"').append(header[i].getName()).append('"').append(':');
            if (Type.isNumeric(header[i].getType())) {
                data.append(values[i]);
            } else {
                data.append('"').append(values[i]).append('"');
            }
            return data.toString();
        }).toArray(String[]::new);
        builder.append(StringUtils.join(", ", arr));
        builder.append('}');
        return builder.toString();
    }

    @Override
    public Map<String, Object> toNamedMap() {
        return IntStream.range(0, size)
                .mapToObj(i -> new Node<>(header[i].getName(), values[i]))
                .collect(Collectors.toMap(Node::getKey, Node::getValue, (k1, k2) -> k1));
    }

    @Override
    public Map<Schema, Object> toMap() {
        return IntStream.range(0, size)
                .mapToObj(i -> new Node<>(header[i], values[i]))
                .collect(Collectors.toMap(Node::getKey, Node::getValue, (k1, k2) -> k1));
    }

    @Override
    public Map<Schema, Object> toMap(int startIndex, int num) {
        rangeCheck(startIndex);
        rangeCheck(startIndex + num);
        // 转化为 Map
        return IntStream.range(startIndex, startIndex + num)
                .mapToObj(i -> new Node<>(header[i], values[i]))
                .collect(Collectors.toMap(Node::getKey, Node::getValue, (k1, k2) -> k1));
        /*rangeCheck(startIndex);
        rangeCheck(endIndex);
        if (startIndex < endIndex)
            throw new IllegalArgumentException(
                    "The parameter startIndex must be bigger than endIndex (" + startIndex+ " - " + endIndex + ")");
        if (startIndex == 0 && endIndex == size) {
            return toMap();
        } else {
            Map<Schema, Object> map = new HashMap<>();
            for (int i = startIndex; i < endIndex; i++) {
                map.put(header[i], getValue(i).getValue());
            }
            return map;
        }*/
    }

    @Override
    public Map<Schema, Object> toMap(@NotNull String... fieldNames) {
        if (fieldNames.length == 0)
            return toMap();
        if ("*".equals(fieldNames[0]))
            return toMap();
        int length = fieldNames.length;
        Map<Schema, Object> map = new HashMap<>();
        for (int i = 0; i < length; i++) {
            int index = find(fieldNames[i]);
            if (index > 0) {
                map.put(header[index], getValue(index).getValue());
            } else {
                throw new IllegalArgumentException(
                        "header has not contains field as ([" + i + "] " + fieldNames[i] + ").");
            }
        }
        return map;
    }

    @Override
    public Map<Schema, Object> toMap(boolean primary) {
        return IntStream.range(0, size).filter(i -> header[i].isPrimary() == primary)
                .mapToObj(i -> new Node<>(header[i], values[i]))
                .collect(Collectors.toMap(Node::getKey, Node::getValue, (k1, k2) -> k1));
    }

    @Override
    public final Object setValue(int index, Object value) {
        rangeCheck(index);
        Object old =  values[index];
        if (value == null) {
            values[index] = Parser.defaultValue(header[index].getType());
        } else {
            if (!Parser.matches(header[index].getType(), value))
                throw new IllegalArgumentException("The Value (" + value + "[ " + value.getClass().getName() +
                                "]) can not match the type of ([" + header[index] + "]).");
            values[index] = value;
        }
        return old;
    }

    @Override
    public Object setValue(String fieldName, Object value) {
        return setValue(find(fieldName), value);
    }

    @Override
    public final Row split(int startIndex, int endIndex) {
        rangeCheck(startIndex);
        rangeCheck(endIndex);
        if (startIndex < endIndex)
            throw new IllegalArgumentException(
                    "The parameter startIndex must be bigger than endIndex (" + startIndex+ " - " + endIndex + ")");
        if (startIndex == 0 && endIndex == size) {
            return new DataRow(header.clone(), values.clone());
        } else {
            return new DataRow(
                    Arrays.copyOfRange(header, startIndex,  endIndex),
                    Arrays.copyOfRange(values, startIndex,  endIndex )
            );
        }
    }

    @Override
    public final Row split(String... fieldNames) {
        if (fieldNames.length == 0)
            return new DataRow(header.clone(), values.clone());
        if ("*".equals(fieldNames[0]))
            return new DataRow(header.clone(), values.clone());
        int length = fieldNames.length;
        Schema [] k = new Schema[length];
        Object [] v = new Object[length];
        for (int i = 0; i < length; i++) {

            int index = find(fieldNames[i]);
            if (index > 0) {
                k[i] = this.header[index];
                v[i] = this.values[index];
            } else {
                throw new IllegalArgumentException(
                        "header has not contains field as ([" + i + "] " + fieldNames[i] + ").");
            }
        }
        return new DataRow(k, v);
    }

    @Override
    public final String toString(char delimiter) {
        return join(delimiter, true);
    }

    @Override
    public final String toString(String delimiter) {
        return join(delimiter, true);
    }

    @Override
    public final String toString() {
        return '[' + join(", ", true) + ']';
    }

    @Override
    public final Value[] values() {
        return IntStream.range(0, size)
                .mapToObj(i -> Parser.parseValue(header[i], values[i])).toArray(Value[]::new);
        /*Value[] retval = new Value[size];
        for (int i = 0; i < size; i++) {
            retval[i] = Parser.parseValue(header[i].getType(), values[i]);
        }
        return retval;*/
    }

    @Override
    public final Value[] values(boolean primary) {
        return IntStream.range(0, size).filter(i -> header[i].isPrimary() == primary)
                .mapToObj(i -> Parser.parseValue(header[i], values[i])).toArray(Value[]::new);
        //Value[] retval = new Value[size];
        //for (int i = 0; i < size; i++) {
        //    if ()
        //    retval[i] = Parser.parse(header[i].getType(), values[i]);
        //}
        //return retval;
    }

    private final static class Node<K, V> {
        K key;
        V value;

        Node(K key, V value) {
            this.key = key;
            this.value = value;
        }

        K getKey(){
            return key;
        }

        V getValue() {
            return value;
        }
    }

}
