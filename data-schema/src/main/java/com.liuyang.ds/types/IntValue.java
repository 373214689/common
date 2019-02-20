package com.liuyang.ds.types;

import com.liuyang.ds.NumberValue;
import com.liuyang.ds.Parser;
import com.liuyang.ds.Type;
import com.sun.istack.internal.NotNull;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;

public final class IntValue extends PrimitiveValue implements NumberValue {

    public static IntValue create() {
        return new IntValue();
    }

    public static IntValue create(int value) {
        return new IntValue(value);
    }

    public static IntValue parse(String value) {
        return new IntValue(Parser.parseInt(value));
    }

    private volatile int element;

    private IntValue(int value) {
        super(Type.INT);
        this.element = value;
    }

    private IntValue() {
        super(Type.INT);
        this.element = 0;
    }

    protected void finalize() {
        element = 0;
    }

    @Override
    public boolean equals(Object anObject) {
        if (anObject == this) return true;
        if (anObject == null) return false;
        if (anObject instanceof IntValue) {
            return element == ((IntValue) anObject).element;
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
        return element;
    }

    @Override
    public final long getLong() {
        return (long) element;
    }

    @Override
    public final short getShort() {
        return (short) element;
    }

    public final String getString() {
        return Integer.toString(element);
    }

    @Override
    public final Integer getValue() {
        return element;
    }

    @Override
    public synchronized final void setValue(int value) {
        element = value;
    }

    @Override
    public synchronized final void setValue(long value) {
        element = (int) value;
    }

    @Override
    public synchronized final void setValue(double value) {
        element = (int) value;
    }

    @Override
    public synchronized final void setValue(float value) {
        element = (int) value;
    }

    @Override
    public synchronized final void setValue(short value) {
        element = (int) value;
    }

    @Override
    public synchronized final void setValue(PrimitiveValue value) {
        element = value == null ? 0 : value.getInteger();
    }

    @Override
    public synchronized final void setValue(Object value) {
        element = Parser.parseInt(value);
    }

    @Override
    public synchronized final void setValue(boolean value) {
        element = value ? 1 : 0;
    }

    @Override
    public synchronized final void setValue(byte[] bytes) {
        element = Parser.parseInt(bytes);
    }

    @Override
    public final void writeValue(OutputStream o) throws IOException {
        o.write(getBinary());
    }

    @Override
    public synchronized final void setValue(String value) {
        element = Parser.parseInt(value);
    }

    @Override
    public synchronized final void readValue(InputStream in) throws IOException {
        int ch1 = in.read();
        int ch2 = in.read();
        int ch3 = in.read();
        int ch4 = in.read();
        if ((ch1 | ch2 | ch3 | ch4) < 0)
            throw new EOFException();
        element = ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4));
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
        return element;
    }

    @Override
    public final long longValue() {
        return (long) element;
    }

    @Override
    public final short shortValue() {
        return (short) element;
    }

    /**
     * Compute
     * @param action 计算表达式
     */
    public synchronized final void compute(IntFunction<Integer> action) {
        element = action.apply(element);
    }

    /**
     * Filter
     * @param action 过滤表达式
     * @return 返回 true 表示匹配，返回 false 表示不匹配。
     */
    public final boolean filter(@NotNull IntPredicate action) {
        return action.test(element);
    }
}
