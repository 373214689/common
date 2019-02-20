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


public final class FloatValue extends PrimitiveValue implements NumberValue {

    public static FloatValue create() {
        return new FloatValue();
    }

    public static FloatValue create(float value) {
        return new FloatValue(value);
    }

    public static FloatValue parse(String value) {
        FloatValue x = new FloatValue();
        x.setValue(value);
        return x;
    }

    private volatile float element;

    private FloatValue(float value) {
        super(Type.FLOAT);
        this.element = value;
    }

    private FloatValue() {
        super(Type.FLOAT);
        this.element = 0;
    }

    protected void finalize() {
        element = 0;
    }

    @Override
    public boolean equals(Object anObject) {
        if (anObject == this) return true;
        if (anObject == null) return false;
        if (anObject instanceof FloatValue) {
            return element == ((FloatValue) anObject).element;
        }
        if (anObject instanceof Number) {
            return element == ((Number) anObject).floatValue();
        }
        if (anObject instanceof NumberValue) {
            return element == ((NumberValue) anObject).floatValue();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Float.hashCode(element);
    }

    @Override
    public String toString() {
        return Float.toString(element);
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
        return element;
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

    @Override
    public final String getString() {
        return Float.toString(element);
    }

    @Override
    public final Float getValue() {
        return element;
    }

    @Override
    public synchronized final void setValue(PrimitiveValue value) {
        element = value == null ? 0 : value.getFloat();
    }

    @Override
    public synchronized final void setValue(boolean value) {
        element = value ? 1 : 0;
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
    public synchronized final void setValue(byte[] bytes) {
        element = Parser.parseFloat(bytes);
    }

    @Override
    public synchronized final void setValue(Object value) {
        element = Parser.parseFloat(value);
    }

    @Override
    public synchronized final void setValue(String value) {
        element = Parser.parseFloat(value);
    }

    @Override
    public final void writeValue(OutputStream o) throws IOException {
        o.write(getBinary());
    }

    @Override
    public synchronized final void readValue(InputStream in) throws IOException {
        byte [] buff = new byte[4];
        in.read(buff, 0, 4);
        setValue(buff);
    }

    @Override
    public final double doubleValue() {
        return (double) element;
    }

    @Override
    public final float floatValue() {
        return element;
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
    public synchronized final void compute(@NotNull Function<Float, Float> action) {
        //java.util.concurrent.atomic.AtomicInteger ai;
        element = action.apply(element);
    }

    /**
     * Filter
     * @param action 过滤表达式
     * @return 返回 true 表示匹配， 返回 false 表示不匹配。
     */
    public final boolean filter(@NotNull Predicate<Float> action) {
        return action.test(element);
    }
}
