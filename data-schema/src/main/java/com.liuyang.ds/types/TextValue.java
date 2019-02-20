package com.liuyang.ds.types;

import com.liuyang.ds.Parser;
import com.liuyang.ds.Type;
import com.liuyang.tools.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public final class TextValue extends PrimitiveValue {

    public static TextValue create() {
        return new TextValue();
    }

    public static TextValue create(String value) {
        return new TextValue(value);
    }

    public static TextValue parse(String value) {
        return new TextValue(value);
    }

    private volatile String element;

    private TextValue(String value) {
        super(Type.STRING);
        this.element = value;
    }

    private TextValue() {
        super(Type.STRING);
        this.element = "";
    }

    @Override
    protected void finalize() {
        type    = null;
        element = null;
    }

    @Override
    public boolean equals(Object anObject) {
        if (anObject == this) return true;
        if (anObject == null) return false;
        if (anObject instanceof TextValue) {
            TextValue other = (TextValue) anObject;
            if (other.element == null)
                return false;
            else
                return other.element.equals(element);
        }
        if (anObject instanceof String) {
            return anObject.equals(element);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return element.hashCode();
    }

    @Override
    public String toString() {
        return element;
    }

    @Override
    public final byte[] getBinary() {
        return element.getBytes();
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
        String value = StringUtils.isEmpty(element) ? "0" : element;
        return Short.parseShort(value);
    }

    public final String getString() {
        return element;
    }

    @Override
    public final void setValue(int value) {
        element = String.valueOf(value);
    }

    @Override
    public final String getValue() {
        return element;
    }

    @Override
    public final void setValue(long value) {
        element = String.valueOf(value);
    }

    @Override
    public final void setValue(double value) {
        element = String.valueOf(value);
    }

    @Override
    public final void setValue(float value) {
        element = String.valueOf(value);
    }

    @Override
    public final void setValue(short value) {
        element = String.valueOf(value);
    }

    @Override
    public final void setValue(byte[] value) {
        element = new String(value);
    }

    @Override
    public final void setValue(String value) {
        element = value;
    }

    @Override
    public final void setValue(PrimitiveValue value) {
        element = value == null ? "" : value.getString();
    }

    @Override
    public final void setValue(Object value) {
        element = Parser.parseString(value);
    }

    @Override
    public final void setValue(boolean value) {
        element = value ? "true" : "false";
    }

    @Override
    public final void writeValue(OutputStream o) throws IOException {
        o.write(getBinary());
    }

    @Override
    public final void readValue(InputStream in) throws IOException {
        byte[] buff = new byte[1024];
        in.read(buff, 0, 1024);
        setValue(buff);
    }
}