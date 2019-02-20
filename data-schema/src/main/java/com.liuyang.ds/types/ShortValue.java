package com.liuyang.ds.types;

import com.liuyang.ds.NumberValue;
import com.liuyang.ds.Parser;
import com.liuyang.ds.Type;
import com.sun.istack.internal.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.Function;
import java.util.function.Predicate;

public final class ShortValue extends PrimitiveValue implements NumberValue {

    public static ShortValue create() {
        return new ShortValue();
    }

    public static ShortValue create(short value) {
        return new ShortValue(value);
    }

    public static ShortValue parse(String value) {
        return new ShortValue(Parser.parseShort(value));
    }

    private volatile short element;

    private ShortValue(short value) {
        super(Type.SHORT);
        this.element = value;
    }

    private ShortValue() {
        super(Type.SHORT);
        this.element = 0;
    }

    protected void finalize() {
        type    = null;
        element = 0;
    }

    @Override
    public boolean equals(Object anObject) {
        if (anObject == this) return true;
        if (anObject == null) return false;
        if (anObject instanceof ShortValue) {
            return element == ((ShortValue) anObject).element;
        }
        if (anObject instanceof Number) {
            return element == ((Number) anObject).intValue();
        }
        if (anObject instanceof NumberValue) {
            return element == ((NumberValue) anObject).intValue();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return element;
    }

    @Override
    public String toString() {
        return Integer.toString(element);
    }

    @Override
    public final byte[] getBinary() {
        return Parser.parseBinary(element);
    }

    @Override
    public final boolean getBoolean() {
        return element <= 0;
    }

    @Override
    public final double getDouble() {
        return (double) element;
    }

    @Override
    public final float getFloat() {
        return (float) element;
    }

    @Override
    public final int getInteger() {
        return (int) element;
    }

    @Override
    public final long getLong() {
        return (long) element;
    }

    @Override
    public final short getShort() {
        return element;
    }

    public final String getString() {
        return Integer.toString(element);
    }

    @Override
    public final Short getValue() {
        return element;
    }

    @Override
    public final void setValue(int value) {
        element = (short) value;
    }

    @Override
    public final void setValue(long value) {
        element = (short) value;
    }

    @Override
    public final void setValue(double value) {
        element = (short) value;
    }

    @Override
    public final void setValue(float value) {
        element = (short) value;
    }

    @Override
    public final void setValue(short value) {
        element = value;
    }

    @Override
    public final void setValue(byte[] bytes) {
        element = Parser.parseShort(bytes);
    }

    @Override
    public final void setValue(String value) {
        element = Parser.parseShort(value);
    }

    @Override
    public final void setValue(PrimitiveValue value) {
        element = value == null ? 0 : value.getShort();
    }

    @Override
    public final void setValue(Object value) {
        element = Parser.parseShort(value);
    }

    @Override
    public final void setValue(boolean value) {
        element = (short) (value ? 1 : 0);
    }

    @Override
    public final void writeValue(OutputStream o) throws IOException {
        o.write(getBinary());
    }

    @Override
    public final void readValue(InputStream in) throws IOException {
        byte[] buff = new byte[4];
        in.read(buff, 0, 4);
        setValue(buff);
    }

    @Override
    public final double doubleValue() {
        return (double) element;
    }

    @Override
    public final float floatValue() {
        return (float) element;
    }

    @Override
    public final int intValue() {
        return (int) element;
    }

    @Override
    public final long longValue() {
        return (long) element;
    }

    @Override
    public final short shortValue() {
        return element;
    }

    /**
     * Compute
     *
     * @param action 运算表达式
     */
    public synchronized final void compute(@NotNull Function<Short, Short> action) {
        element = action.apply(element);
    }

    /**
     * Filter
     *
     * @param action 过滤表达式
     * @return 返回 true 表示匹配，返回 false 表示不匹配。
     */
    public final boolean filter(@NotNull Predicate<Short> action) {
        return action.test(element);
    }
}
