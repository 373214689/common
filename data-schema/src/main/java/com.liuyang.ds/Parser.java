package com.liuyang.ds;

import com.liuyang.ds.types.*;
import com.liuyang.tools.StringUtils;
import com.sun.istack.internal.NotNull;

/**
 * 数据解析器
 * <ul>
 *     <li>2019/1/4   ver 1.0.0 created.</li>
 *     <li>2019/1/21  ver 1.0.1 add function. <code>matches</code>.</li>
 * </ul>
 * @author liuyang
 * @version 1.0.1
 */
public final class Parser {

    // 处理异常
    private static IllegalArgumentException exception(Object value, String type) {
        return new IllegalArgumentException(
                "Illegal parameter [" + value.getClass() + "]: " + value + ", can not matched type (" + type + ").");
    }

    // 复制数组
    private static byte[] copy(byte[] element, int length) {
        int elen = element.length;
        byte[] bytes = new byte[length];
        if (elen > length) {
            System.arraycopy(element, elen - length, bytes, 0, length);
        } else if (elen < length) {
            System.arraycopy(element, 0, bytes, length - elen, elen);
        } else {
            bytes = element;
        }
        return bytes;
    }

    /**
     * 获取指定类型的默认值。
     * <p>
     *     此项主要是为了解决 Type 与 null 之间的 NullPointerException 问题。
     * </p>
     * @param type 指定类型。
     * @return 返回对应类型的默认值。
     */
    public static Object defaultValue(Type type) {
        switch(type) {
            case BINARY: {
                return new byte[]{};
            }
            case BOOL:
            case BOOLEAN: {
                return false;
            }
            case DOUBLE: {
                return 0.00D;
            }
            case FLOAT: {
                return 0.00F;
            }
            case INT:
            case INTEGER: {
                return 0;
            }
            case BIGINT:
            case LONG: {
                return 0L;
            }
            case TINYINT:
            case SMALLINT:
            case SHORT: {
                return (short) 0;
            }
            case VARCHAR:
            case STRING: {
                return "";
            }
            default:
                return null;
        }
    }

    /**
     * 检测数据与类型是否匹配。
     * <p>
     *     只有类型和数据匹配才能对原数据进行修改，不匹配就修改的情况下，会造成不必要的错误。
     * </p>
     * @param type 指定类型
     * @param value 指定数据
     * @return 返回 true 表示匹配， 返回 false 表示不匹配。
     */
    public static boolean matches(Type type, Object value) {
        if (type == null || value == null)
            return false;
        switch(type) {
            case BINARY: {
                return (value instanceof byte[]);
            }
            case BOOL:
            case BOOLEAN: {
                return (value instanceof Boolean);
            }
            case DOUBLE: {
                return (value instanceof Number);
            }
            case FLOAT: {
                return (value instanceof Number);
            }
            case INT:
            case INTEGER: {
                return (value instanceof Number);
            }
            case BIGINT:
            case LONG: {
                return (value instanceof Number);
            }
            case TINYINT:
            case SMALLINT:
            case SHORT: {
                return (value instanceof Number);
            }
            case VARCHAR:
            case STRING: {
                return (value instanceof String);
            }
            default:
                throw exception(value, "undefined");
        }
    }

    /**
     * 解析文本数据为指定类型的数据。
     * @param type 指定类型
     * @param value 待解析的文本数据
     * @return 返回解析后的数据
     */
    public static Object parse(@NotNull Type type, String value) {
        switch(type) {
            case BINARY: {
                return parseBinary(value); // 可以考虑使用StringUtil.asBytes
            }
            case BOOL:
            case BOOLEAN: {
                return parseBoolean(value);
            }
            case DOUBLE: {
                return parseDouble(value);
            }
            case FLOAT: {
                return parseFloat(value);
            }
            case INT:
            case INTEGER: {
                return parseInt(value);
            }
            case BIGINT:
            case LONG: {
                return parseLong(value);
            }
            case TINYINT:
            case SMALLINT:
            case SHORT: {
                return parseShort(value);
            }
            case VARCHAR:
            case STRING: {
                return value;
            }
            default:
                throw exception(value, "undefined");
        }
    }

    public static Object parse(@NotNull Schema schema, String value) {
        return parse(schema.getType(), value);
    }

    /**
     * 将字节数据解析为指定类型的数据
     * <p>
     *     <i>不排除会遇到无法正确解析的字节，因此需要事先规划好字节数据。</i>
     * </p>
     * @param type 指定的类型 <code>com.liuyang.ds.Type</code>
     * @param value 字节数据，不限制长度
     * @return 返回值，其类型为<code>Value</code>，通过Value可以进行更高级的操作。
     * @see Type
     * @see Value
     */
    public static Value parseValue(@NotNull Type type, @NotNull byte[] value) {
        switch(type) {
            case BINARY: {
                return BinaryValue.create((byte[]) value);
            }
            case BOOL:
            case BOOLEAN: {
                return BooleanValue.create(parseBoolean(value));
            }
            case DOUBLE: {
                return DoubleValue.create(parseDouble(value));
            }
            case FLOAT: {
                return FloatValue.create(parseFloat(value));
            }
            case INT:
            case INTEGER: {
                return IntValue.create(parseInt(value));
            }
            case BIGINT:
            case LONG: {
                return LongValue.create(parseLong(value));
            }
            case TINYINT:
            case SMALLINT:
            case SHORT: {
                return ShortValue.create(parseShort(value));
            }
            case VARCHAR:
            case STRING: {
                return TextValue.create(new String(value));
            }
            default:
                throw exception(value, "undefined");
        }
    }

    public static Value parseValue(@NotNull Schema schema, @NotNull byte[] value) {
        return parseValue(schema.getType(), value);
    }

    /**
     * 将字符串数据解析为指定类型的数据
     * <p>
     *     <i>不排除会遇到无法正确解析的字符串，因此需要事先规划好字符串数据。</i>
     * </p>
     * @param type 指定的类型 <code>com.liuyang.ds.Type</code>
     * @param value 字符串数据，不限制长度
     * @return 返回值，其类型为<code>Value</code>，通过<code>Value</code>可以进行更高级的操作。
     * @see Type
     * @see Value
     */
    public static Value parseValue(@NotNull Type type, String value) {
        switch(type) {
            case BINARY: {
                return BinaryValue.create(parseBinary(value)); // 可以考虑使用StringUtil.asBytes
            }
            case BOOL:
            case BOOLEAN: {
                return BooleanValue.create(parseBoolean(value));
            }
            case DOUBLE: {
                return DoubleValue.create(parseDouble(value));
            }
            case FLOAT: {
                return FloatValue.create(parseFloat(value));
            }
            case INT:
            case INTEGER: {
                return IntValue.create(parseInt(value));
            }
            case BIGINT:
            case LONG: {
                return LongValue.create(parseLong(value));
            }
            case TINYINT:
            case SMALLINT:
            case SHORT: {
                return ShortValue.create(parseShort(value));
            }
            case VARCHAR:
            case STRING: {
                return TextValue.create(value);
            }
            default:
                throw exception(value, "undefined");
        }
    }

    public static Value parseValue(@NotNull Schema schema, String value) {
        return parseValue(schema.getType(), value);
    }

    public static Value parseValue(@NotNull Type type, @NotNull Object value) {
        switch(type) {
            case BINARY: {
                return BinaryValue.create(parseBinary(value));
            }
            case BOOL:
            case BOOLEAN: {
                return BooleanValue.create(parseBoolean(value));
            }
            case DOUBLE: {
                return DoubleValue.create(parseDouble(value));
            }
            case FLOAT: {
                return FloatValue.create(parseFloat(value));
            }
            case INT:
            case INTEGER: {
                return IntValue.create(parseInt(value));
            }
            case BIGINT:
            case LONG: {
                return LongValue.create(parseLong(value));
            }
            case TINYINT:
            case SMALLINT:
            case SHORT: {
                return ShortValue.create(parseShort(value));
            }
            case VARCHAR:
            case STRING: {
                return TextValue.create(parseString(value));
            }
            default:
                throw exception(value, "undefined");
        }
    }

    public static Value parseValue(@NotNull Schema schema, @NotNull Object value) {
        return parseValue(schema.getType(), value);
    }

    public static boolean parseBoolean(@NotNull byte[] value) {
        return value[0] != 0 ;
    }

    public static boolean parseBoolean(String value) {
        return Boolean.parseBoolean(value);
    }

    public static boolean parseBoolean(Object value) {
        if (value instanceof byte[])
            return parseBoolean((byte[]) value);
        if (value instanceof Boolean)
            return (boolean) value;
        if (value instanceof Number)
            return ((Number) value).intValue() > 0;
        if (value instanceof NumberValue)
            return ((NumberValue) value).intValue() > 0;
        if (value instanceof String)
            return parseBoolean((String) value);
        throw exception(value, "Boolean");
    }

    public static byte[] parseBinary(boolean value) {
        return value ? new byte[] { 1 } : new byte[] { 0 };
    }

    public static byte[] parseBinary(double value) {
        return parseBinary(Double.doubleToLongBits(value));
    }

    public static byte[] parseBinary(float value) {
        return parseBinary(Float.floatToIntBits(value));
    }

    public static byte[] parseBinary(int value) {
        return new byte[] {
                (byte) ((value >> 24) & 0xFF),
                (byte) ((value >> 16) & 0xFF),
                (byte) ((value >> 8) & 0xFF),
                (byte) (value & 0xFF)
        };
    }

    public static byte[] parseBinary(long value) {
        return new byte[] {
                (byte) ((value >> 56) & 0xFF),
                (byte) ((value >> 48) & 0xFF),
                (byte) ((value >> 40) & 0xFF),
                (byte) ((value >> 32) & 0xFF),
                (byte) ((value >> 24) & 0xFF),
                (byte) ((value >> 16) & 0xFF),
                (byte) ((value >> 8) & 0xFF),
                (byte) (value & 0xFF)
        };
    }

    public static byte[] parseBinary(short value) {
        return new byte[] {
                (byte) ((value >> 8) & 0xFF),
                (byte) (value & 0xFF)
        };
    }

    public static byte[] parseBinary(String value) {
        return StringUtils.vaild(value, "").getBytes();
    }

    public static byte[] parseBinary(Object value) {
        if (value instanceof Boolean)
            return parseBinary((boolean) value);
        if (value instanceof Double)
            return parseBinary((double) value);
        if (value instanceof Float)
            return parseBinary((float) value);
        if (value instanceof Integer)
            return parseBinary((int) value);
        if (value instanceof Long)
            return parseBinary((long) value);
        if (value instanceof Short)
            return parseBinary((short) value);
        if (value instanceof String)
            return ((String) value).getBytes();
        if (!(value instanceof byte[]))
            return (byte[]) value;
        throw exception(value, "byte[]");
    }



    public static double parseDouble(@NotNull byte[] value) {
        //java.io.DataInputStream dis;
        /*if ((x > 0x7ff0000000000001L && x <= 0x7fffffffffffffffL)
        		|| (x >= 0xfff0000000000001L && x <= 0xffffffffffffffffL)) {
        	element = Double.longBitsToDouble(x);
        } else {
        	element = (double) x;
        }*/
        return Double.longBitsToDouble(parseLong(value));
    }

    public static double parseDouble(String value) {
        if (!StringUtils.isNumeric(value))
            return 0;
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    public static double parseDouble(Object value) {
        if (value instanceof Double)
            return (double) value;
        if (value instanceof Number)
            return ((Number) value).doubleValue();
        if (value instanceof NumberValue)
            return ((NumberValue) value).doubleValue();
        throw exception(value, "Double");
    }

    public static float parseFloat(@NotNull byte[] value) {
        return Float.intBitsToFloat(parseInt(value));
    }

    public static float parseFloat(String value) {
        if (!StringUtils.isNumeric(value))
            return 0;
        try {
            return Float.parseFloat(value);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    public static float parseFloat(Object value) {
        if (value instanceof Float)
            return (float) value;
        if (value instanceof Number)
            return ((Number) value).floatValue();
        if (value instanceof NumberValue)
            return ((NumberValue) value).floatValue();
        throw exception(value, "Float");
    }

    public static int parseInt(@NotNull byte[] value) {
        byte[] bytes = copy(value, 4);
        return (0xff & bytes[3])
                | (0xff00 & (bytes[2] << 8))
                | (0xff0000 & (bytes[1] << 16))
                | (0xff000000 & (bytes[0] << 24));
    }

    public static int parseInt(String value) {
        if (!StringUtils.isNumeric(value))
            return 0;
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    public static int parseInt(Object value) {
        if (value instanceof Integer)
            return (int) value;
        if (value instanceof Number)
            return ((Number) value).intValue();
        if (value instanceof NumberValue)
            return ((NumberValue) value).intValue();
        throw exception(value, "Integer");
    }

    public static long parseLong(@NotNull byte[] value) {
        int length = 8;
        byte[] bytes = copy(value, 8);
        long x = 0;
        for (int i = 0; i < length; i++) {
            x <<= length;
            x |= (bytes[i] & 0xff);
        }
        return x;
    }

    public static long parseLong(String value) {
        if (!StringUtils.isNumeric(value))
            return 0L;
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    public static long parseLong(Object value) {
        if (value instanceof Long)
            return (long) value;
        if (value instanceof Number)
            return ((Number) value).longValue();
        if (value instanceof NumberValue)
            return ((NumberValue) value).longValue();
        throw exception(value, "Long");
    }

    public static short parseShort(@NotNull byte[] value) {
        byte[] bytes = copy(value, 2);
        return (short) ((0xff & bytes[1])
                | (0xff00 & (bytes[0] << 8)));
    }

    public static short parseShort(String value) {
        if (!StringUtils.isNumeric(value))
            return (short) 0;
        try {
            return Short.parseShort(value);
        } catch (NumberFormatException e) {
            return (short) 0;
        }
    }

    public static short parseShort(Object value) {
        if (value instanceof Short)
            return (short) value;
        if (value instanceof Number)
            return ((Number) value).shortValue();
        if (value instanceof NumberValue)
            return ((NumberValue) value).shortValue();
        throw exception(value, "Short");
    }

    public static String parseString(byte[] value) {
        return new String(value);
    }

    public static String parseString(Object value) {
        return String.valueOf(value);
    }


}
