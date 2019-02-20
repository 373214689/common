package com.liuyang.ds.types;

import com.liuyang.ds.NumberValue;
import com.liuyang.ds.Parser;
import com.liuyang.ds.Type;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public final class BooleanValue extends PrimitiveValue {

    public static BooleanValue create() {
        return new BooleanValue();
    }

    public static BooleanValue create(boolean value) {
        return new BooleanValue(value);
    }

    public static BooleanValue parse(String value) {
        return new BooleanValue(Parser.parseBoolean(value));
    }

    private volatile boolean element;

    private BooleanValue(boolean value) {
        super(Type.BOOLEAN);
        this.element = value;
    }

    private BooleanValue() {
        super(Type.BOOLEAN);
        this.element = false;
    }

    @Override
    protected void finalize() {
        element = false;
    }

    @Override
    public boolean equals(Object anObject) {
        if (anObject == this) return true;
        if (anObject == null) return false;
        if (anObject instanceof BooleanValue) {
            return element == ((BooleanValue) anObject).element;
        }
        if (anObject instanceof NumberValue) {
            return element == (((NumberValue) anObject).intValue() != 0);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Boolean.hashCode(element);
    }

    @Override
    public String toString() {
        return String.valueOf(element);
    }

    @Override
    public final byte[] getBinary() {
        return element ? new byte[] {1} : new byte[] {0};
    }

    @Override
    public final boolean getBoolean() {
        return element;
    }

    @Override
    public final double getDouble() {
        return element ? 1: 0;
    }

    @Override
    public final float getFloat() {
        return element ? 1: 0;
    }

    @Override
    public final int getInteger() {
        return element ? 1: 0;
    }

    @Override
    public final long getLong() {
        return element ? 1: 0;
    }

    @Override
    public final short getShort() {
        return (short) (element ? 1: 0);
    }

    public final String getString() {
        return String.valueOf(element);
    }

    @Override
    public final Boolean getValue() {
        return element;
    }

    @Override
    public synchronized final void setValue(byte[] value) {
        element = Parser.parseBoolean(value);
    }

    @Override
    public synchronized final void setValue(boolean value) {
        element = value;
    }

    @Override
    public synchronized final void setValue(int value) {
        element = value != 0;
    }

    @Override
    public synchronized final void setValue(long value) {
        element = value != 0;
    }

    @Override
    public synchronized final void setValue(double value) {
        element = value != 0;
    }

    @Override
    public synchronized final void setValue(float value) {
        element = value != 0;
    }

    @Override
    public synchronized final void setValue(short value) {
        element = value != 0;
    }

    @Override
    public synchronized final void setValue(String value) {
        element = Parser.parseValue(type, value).getBoolean();
    }

    @Override
    public synchronized final void setValue(PrimitiveValue value) {
        element = value == null ? false : value.getBoolean();
    }

    @Override
    public synchronized final void setValue(Object value) {
        element = Parser.parseBoolean(value);
    }

    @Override
    public synchronized final void writeValue(OutputStream o) throws IOException {
        o.write(getBinary());
    }

    @Override
    public synchronized final void readValue(InputStream in) throws IOException {
        byte [] buff = new byte[1];
        in.read(buff, 0, 1);
        setValue(buff);
    }
}
