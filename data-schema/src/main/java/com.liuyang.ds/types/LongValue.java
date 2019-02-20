package com.liuyang.ds.types;

import com.liuyang.ds.NumberValue;
import com.liuyang.ds.Parser;
import com.liuyang.ds.Type;
import com.liuyang.tools.StringUtils;
import com.sun.istack.internal.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.LongFunction;
import java.util.function.LongPredicate;

/**
 * Long Value
 * @author liuyang
 * @version 1.0.1
 *
 */
public final class LongValue extends PrimitiveValue implements NumberValue {

    public static LongValue create() {
        return new LongValue();
    }

    public static LongValue create(long value) {
        return new LongValue(value);
    }

    public static LongValue parse(String value) {
        LongValue x = new LongValue();
        x.setValue(value);
        return x;
    }

    private volatile long element;

    private LongValue(long value) {
        super(Type.LONG);
        this.element = value;
    }

    private LongValue() {
        super(Type.LONG);
        this.element = 0;
    }

    @Override
    protected void finalize() {
        type    = null;
        element = 0;
    }

    @Override
    public boolean equals(Object anObject) {
        if (anObject == this) return true;
        if (anObject == null) return false;
        if (anObject instanceof LongValue) {
            return element == ((LongValue) anObject).element;
        }
        if (anObject instanceof Number) {
            return element == ((Number) anObject).longValue();
        }
        if (anObject instanceof NumberValue) {
            return element == ((NumberValue) anObject).longValue();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(element);
    }

    @Override
    public String toString() {
        return Long.toString(element);
    }

    @Override
    public final byte[] getBinary() {
        return Parser.parseBinary(element);
    }

    @Override
    public final boolean getBoolean() {
        return element > 0;
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
        return element;
    }

    @Override
    public final short getShort() {
        return (short) element;
    }

    public final String getString() {
        return Long.toString(element);
    }

    @Override
    public final Long getValue() {
        return element;
    }

    @Override
    public final void setValue(int value) {
        element = (long) value;
    }

    @Override
    public final void setValue(long value) {
        element = value;
    }

    @Override
    public final void setValue(double value) {
        element = (long) value;
    }

    @Override
    public final void setValue(float value) {
        element = (long) value;
    }

    @Override
    public final void setValue(short value) {
        element = (long) value;
    }

    @Override
    public final void setValue(byte[] value) {
        element = Parser.parseLong(value);
    }

    @Override
    public final void setValue(String value) {
        element = Long.parseLong(StringUtils.vaild(value, "0"));
    }

    @Override
    public final void setValue(PrimitiveValue value) {
        element = value == null ? 0 : value.getLong();
    }

    @Override
    public final void setValue(Object value) {
        element = Parser.parseLong(value);
    }

    @Override
    public final void setValue(boolean value) {
        element = value ? 1 : 0;
    }

    @Override
    public final void writeValue(OutputStream o) throws IOException {
        o.write(getBinary());
    }

    @Override
    public final void readValue(InputStream in) throws IOException {
        byte [] buff = new byte[8];
        in.read(buff, 0, 8);
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
        return element;
    }

    @Override
    public final short shortValue() {
        return (short) element;
    }

    /**
     * Compute
     * @param action 运算表达式
     */
    public synchronized final void compute(@NotNull LongFunction<Long> action) {
        element = action.apply(element);
    }

    /**
     * Filter
     * @param action 过滤表达式
     * @return 返回 true 表示匹配，返回 false 表示不匹配。
     */
    public final boolean filter(@NotNull LongPredicate action) {
        return action.test(element);
    }
}
