package com.liuyang.ds;

/**
 * Number Value Interface.
 *
 * @author liuyang
 * @version 1.0.0
 */
public interface NumberValue {

    /**
     * Get double value.
     * @return Return a double value.
     */
    double doubleValue();

    /**
     * Get float value.
     * @return Return a float value.
     */
    float floatValue();

    /**
     * Get int value.
     * @return Return a int value.
     */
    int  intValue();

    /**
     * Get long value.
     * @return Return a long value.
     */
    long longValue();

    /**
     * Get short value.
     * @return Return a short value.
     */
    short shortValue();

    /**
     * Get byte value。
     * @return Return a byte value.
     */
    default byte byteValue() {
        return (byte) intValue();
    }
}
