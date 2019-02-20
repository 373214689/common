package com.liuyang.ds.types;

import com.liuyang.ds.NumberValue;
import com.liuyang.ds.Parser;
import com.liuyang.ds.Type;
import com.sun.istack.internal.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.DoubleFunction;
import java.util.function.DoublePredicate;

public final class DoubleValue extends PrimitiveValue implements NumberValue {

    public static DoubleValue create() {
        return new DoubleValue();
    }

    public static DoubleValue create(double value) {
        return new DoubleValue(value);
    }

    public static DoubleValue parse(String value) {
        DoubleValue x = new DoubleValue();
        x.setValue(value);
        return x;
    }

    private volatile double element;

    private DoubleValue(double value) {
        super(Type.DOUBLE);
        this.element = value;
    }

    private DoubleValue() {
        super(Type.DOUBLE);
        this.element = 0.00;
    }

    protected void finalize() {
        element = 0.00;
    }

    @Override
    public boolean equals(Object anObject) {
        if (anObject == this) return true;
        if (anObject == null) return false;
        if (anObject instanceof DoubleValue) {
            return element == ((DoubleValue) anObject).element;
        }
        if (anObject instanceof Number) {
            return element == ((Number) anObject).doubleValue();
        }
        if (anObject instanceof NumberValue) {
            return element == ((NumberValue) anObject).doubleValue();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Double.hashCode(element);
    }

    @Override
    public String toString() {
        return Double.toString(element);
    }

    @Override
    public final byte[] getBinary() {
        long x = Double.doubleToLongBits(element);
        return new byte[] {
                (byte) ((x >> 56) & 0xFF),
                (byte) ((x >> 48) & 0xFF),
                (byte) ((x >> 40) & 0xFF),
                (byte) ((x >> 32) & 0xFF),
                (byte) ((x >> 24) & 0xFF),
                (byte) ((x >> 16) & 0xFF),
                (byte) ((x >> 8) & 0xFF),
                (byte) (x & 0xFF)
        };
    }

    @Override
    public final boolean getBoolean() {
        return element != 0;
    }

    @Override
    public final double getDouble() {
        return element;
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
        return (short) element;
    }

    public final String getString() {
        return Double.toString(element);
    }

    @Override
    public final Double getValue() {
        return element;
    }

    @Override
    public synchronized final void setValue(byte[] value) {
        element = Parser.parseDouble(value);
    }

    @Override
    public synchronized final void setValue(boolean value) {
        element = value ? 1.00 : 0.00;
    }

    @Override
    public synchronized final void setValue(int value) {
        element = (long) value;
    }

    @Override
    public synchronized final void setValue(long value) {
        element = value;
    }

    @Override
    public synchronized final void setValue(double value) {
        element = (long) value;
    }

    @Override
    public synchronized final void setValue(float value) {
        element = (long) value;
    }

    @Override
    public synchronized final void setValue(short value) {
        element = (long) value;
    }

    @Override
    public synchronized final void setValue(String value) {
        element = Parser.parseDouble(value);
    }

    @Override
    public synchronized final void setValue(PrimitiveValue value) {
        element = value == null ? 0.00 : value.getDouble();
    }

    @Override
    public synchronized final void setValue(Object value) {
        element = Parser.parseDouble(value);
    }

    @Override
    public synchronized final void writeValue(OutputStream o) throws IOException {
        o.write(getBinary());
    }

    @Override
    public synchronized final void readValue(InputStream in) throws IOException {
        byte [] buff = new byte[8];
        in.read(buff, 0, 8);
        setValue(buff);
    }

    @Override
    public final double doubleValue() {
        return element;
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
        return (short) element;
    }

    /**
     * Compute
     * @param action 运算表达式
     */
    public synchronized final void compute(@NotNull DoubleFunction<Double> action) {
        element = action.apply(element);
    }

    /**
     * Filter
     * @param action 过滤表达式
     * @return 返回 true 表示匹配，返回 false 表示不匹配。
     */
    public final boolean filter(@NotNull DoublePredicate action) {
        return action.test(element);
    }
}
