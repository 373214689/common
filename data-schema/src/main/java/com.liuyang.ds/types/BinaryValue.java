package com.liuyang.ds.types;

import com.liuyang.ds.Parser;
import com.liuyang.ds.Type;
import com.liuyang.tools.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

public final class BinaryValue extends PrimitiveValue {

    public static BinaryValue create() {
        return new BinaryValue();
    }

    public static BinaryValue create(byte[] value) {
        return new BinaryValue(value);
    }

    public static BinaryValue parse(String value) {
        BinaryValue x = new BinaryValue();
        x.setValue(value);
        return x;
    }

    private volatile byte[] element;
    private volatile int length = 0;

    private BinaryValue(byte[] value) {
        super(Type.BINARY);
        this.element = value;
    }

    private BinaryValue() {
        super(Type.BINARY);
        this.element = new byte[] {};
    }

    @Override
    protected void finalize() {
        element = null;
    }

    @Override
    public boolean equals(Object anObject) {
        if (anObject == this) return true;
        if (anObject == null) return false;
        if (anObject instanceof BinaryValue) {
            return element == ((BinaryValue) anObject).element;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(element);
    }

    @Override
    public String toString() {
        return StringUtils.join(' ', element);
        //return element.toString();
    }

    @Override
    public final byte[] getBinary() {
        return element;
    }

    @Override
    public final boolean getBoolean() {
        return Parser.parseBoolean(element);
    }

    @Override
    public final double getDouble() {
        return Parser.parseDouble(element);
    }

    @Override
    public final float getFloat() {
        return Parser.parseFloat(element);
    }

    @Override
    public final int getInteger() {
        return Parser.parseInt(element);
    }

    @Override
    public final long getLong() {
        return Parser.parseLong(element);
    }

    @Override
    public final short getShort() {
         return Parser.parseShort(element);
    }

    public final String getString() {
        return new String(element);
    }

    @Override
    public final byte[] getValue() {
        return element;
    }

    @Override
    public synchronized final void setValue(byte[] value) {
        length = value.length;
        element = value;
    }

    @Override
    public synchronized final void setValue(boolean value) {
        length = 1;
        element = Parser.parseBinary(value);
    }

    @Override
    public synchronized final void setValue(int value) {
        length = 4;
        element = Parser.parseBinary(value);
    }

    @Override
    public synchronized final void setValue(long value) {
        length = 8;
        element = Parser.parseBinary(value);
    }

    @Override
    public synchronized final void setValue(double value) {
        length = 8;
        element = Parser.parseBinary(value);
    }

    @Override
    public synchronized final void setValue(float value) {
        length = 4;
        element = Parser.parseBinary(value);
    }

    @Override
    public synchronized final void setValue(short value) {
        length = 2;
        element = Parser.parseBinary(value);
    }

    @Override
    public synchronized final void setValue(String value) {
        byte[] bytes = StringUtils.isEmpty(value) ? new byte[] {} : value.getBytes();
        length = bytes.length;
        element = bytes;
    }

    @Override
    public synchronized void setValue(PrimitiveValue value) {
        element = value == null ? new byte[] {} : value.getBinary();
    }

    @Override
    public synchronized final void setValue(Object value) {
        element = Parser.parseBinary(value);
    }

    @Override
    public synchronized final void writeValue(OutputStream o) throws IOException {
        o.write(getBinary());
    }

    @Override
    public synchronized final void readValue(InputStream in) throws IOException {
        if (length > 0) {
            byte [] buff = new byte[length];
            in.read(buff, 0, 1024);
            setValue(buff);
        }
    }
}
