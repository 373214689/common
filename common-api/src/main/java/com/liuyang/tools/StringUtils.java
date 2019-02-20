package com.liuyang.tools;

import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;

/**
 *  String Utility Tool
 * <ul>
 *     <li>2019/1/4 ver1.0.0 LiuYang created.</li>
 *     <li>2019/2/1 ver1.0.1 LiuYang add functions: builder, bytes, count, join{collection, map} and so on.</li>
 * </ul>
 * @author liuyang
 * @version 1.0.1
 */
public class StringUtils {

    /**
     * 创建字符缓冲区。
     * <p>
     *     <code>StringBuffer</code> 可以进行多线程异步操作。
     * </p>
     * @return 返回字符缓冲区。
     */
    public static StringBuffer buffer() {
        return new StringBuffer();
    }

    public static StringBuffer buffer(char c) {
        return buffer().append(c);
    }

    public static StringBuffer buffer(Object o) {
        return buffer().append(o);
    }

    public static StringBuffer buffer(String str) {
        return new StringBuffer(str);
    }

    /**
     * 创建字符缓冲区。
     * @return 返回字符缓冲区。
     */
    public static StringBuilder builder() {
        return new StringBuilder();
    }

    /**
     * 创建字符缓冲区。
     * @param c 指定一个字符
     * @return 返回字符缓冲区。
     */
    public static StringBuilder builder(char c) {
        return builder().append(c);
    }

    /**
     * 创建字符缓冲区。
     * @param o 指定一个对像
     * @return 返回字符缓冲区。
     */
    public static StringBuilder builder(Object o) {
        return builder().append(o);
    }

    /**
     * 创建字符缓冲区。
     * @param str 指定一个字符串。
     * @return 返回字符缓冲区。
     */
    public static StringBuilder builder(String str) {
        return new StringBuilder(str);
    }


    /**
     * 获取字符串字节长度。
     * <p>
     *     ***不同于 <code>String.getBytes::length</code>，此方法不需要进行字符串转换。
     * </p>
     * @param str 指定字符串
     * @return 返回字符串字节长度
     */
    public static int bytes(String str) {
        if (isEmpty(str))
            return 0;
        int length = 0, size = str.length() ;
        for (int i = 0; i < size; i++) {
            int ascii = Character.codePointAt(str, i); //str.charAt(i);
            length += (ascii >= 0 && ascii <= 255) ? 1 : 2;
        }
        return length;
    }

    /**
     * 判断字符串数组包含有指定的字符串。
     * @param source 字符串数组
     * @param target 指定的字符串
     * @return 返回 true 表示包含，返回 false 表示不包含。
    */
    public static boolean contains(String[] source, String target) {
        if (source == null)
            return false;
        for (String str : source) {
            if (equals(str, target))
                return true;
        }
        return false;

    }

    /**
     * 统计字符在指定字符串中出现的次数。
     * @param str 指定字符串
     * @param c 待检索的字符
     * @return 返回字符出现次数
     */
    public static int count(String str, final char c) {
        if (isEmpty(str)) return 0;
        int cnt = 0;
        for (int i = 0, size = str.length(); i < size; i++) {
            if (c == str.charAt(i)) cnt++;
        }
        return cnt;
    }

    /**
     * 统计字符在指定字符串中出现的次数。
     * @param a 指定字符串
     * @param b 待检索的字符串
     * @return 返回字符出现次数
     */
    public static int count(String a, String b) {
        int cnt = 0;
        if (a == null || b == null) return cnt;
        int len1 = a.length();
        int len2 = b.length();
        // 如果 b 的长度为 1，则表示 b 可以使用 char 进行处理。
        if (len2 == 1)
            return count(a, b.charAt(0));
        // 如果 a 的长度小于 b 的长度，则直接返回 0；如果 a 与 b 的长度相等，则判断两者是否相同即可。
        if (len1 < len2) {
            return cnt;
        } else if (len1 == len2) {
            return a.equals(b) ? 1 : 0;
        }
        // 锁定 b 的第一个字符，只要该字符在 a 中出现，则开始检索。
        char first  = b.charAt(0); // 指定第一个字符
        for (int i = 0; i < len1; i++) {
            if (first == a.charAt(i)) {
                int n = 0; // 记录 b 字符集在 a 中的出现次数。
                for (int j = 0; j < len2 && a.charAt(i) == b.charAt(j); j++, i++){n++;}
                if (n == len2) cnt++;
            }
        }
        return cnt;
    }

    /**
     * 判断指定字符串是否相等。如果两个字符串
     * @param a 第一个字符串
     * @param b 第二个字符串
     * @return 返回 true 表示字符串相等，返回 false 表示字符串不相等。
     */
    public static boolean equals(String a, String b) {
        if (a == null) {
            return b == null;
        } else {
            return a.equals(b);
        }
    }

    public static void forEach(String[] arr, Consumer<String> action) {
        if (arr == null)
            return;
        for(String str : arr)
            action.accept(str);
    }

    public static boolean isEmpty(String str) {
        if (str == null) return true;
        return str.isEmpty();
    }

    public static String join(String delimiter, String quoter, byte[] values) {
        if (isEmpty(quoter))
            return join(delimiter, values);
        StringBuilder builder = new StringBuilder();
        for (int i = 0, length = values.length; i < length; i++) {
            if (i == 0) {
                builder.append(quoter).append(String.format("%02X", values[i])).append(quoter);
            } else {
                builder.append(delimiter).append(quoter).append(String.format("%02X", values[i])).append(quoter);
            }
        }
        return builder.toString();
    }

    public static String join(String delimiter, String quoter, String[] values) {
        if (isEmpty(quoter))
            return join(delimiter, values);
        StringBuilder builder = new StringBuilder();
        for (int i = 0, length = values.length; i < length; i++) {
            if (i == 0) {
                builder.append(quoter).append(values[i]).append(quoter);
            } else {
                builder.append(delimiter).append(quoter).append(values[i]).append(quoter);
            }
        }
        return builder.toString();
    }

    public static String join(String delimiter, String quoter, Object[] values) {
        if (isEmpty(quoter))
            return join(delimiter, values);
        StringBuilder builder = new StringBuilder();
        for (int i = 0, length = values.length; i < length; i++) {
            if (i == 0) {
                builder.append(quoter).append(values[i]).append(quoter);
            } else {
                builder.append(delimiter).append(quoter).append(values[i]).append(quoter);
            }
        }
        return builder.toString();
    }

    public static String join(char delimiter, byte[] values) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0, length = values.length; i < length; i++) {
            if (i == 0) {
                builder.append(String.format("%02X", values[i]));
            } else {
                builder.append(delimiter).append(String.format("%02X", values[i]));
            }
        }
        return builder.toString();
    }

    public static String join(String delimiter, byte[] values) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0, length = values.length; i < length; i++) {
            if (i == 0) {
                builder.append(String.format("%02X", values[i]));
            } else {
                builder.append(delimiter).append(String.format("%02X", values[i]));
            }
        }
        return builder.toString();
    }

    public static String join(char delimiter, Object[] values) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0, length = values.length; i < length; i++) {
            if (i == 0) {
                builder.append(values[i]);
            } else {
                builder.append(delimiter).append(values[i]);
            }
        }
        return builder.toString();
    }

    public static String join(String delimiter, boolean parse, Object[] values) {
        StringBuilder builder = new StringBuilder();
        for (Object value : values) {
            if (builder.length() == 0) {
                builder.append(parse ? parseObject(value) : value);
            } else {
                builder.append(delimiter).append(parse ? parseObject(value) : value);
            }
        }
        return builder.toString();
    }

    public static String join(String delimiter, Object[] values) {
        StringBuilder builder = new StringBuilder();
        for (Object value : values) {
            if (builder.length() == 0) {
                builder.append(value);
            } else {
                builder.append(delimiter).append(value);
            }
        }
        return builder.toString();
    }

    public static String join(char delimiter, int[] values) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0, length = values.length; i < length; i++) {
            if (i == 0) {
                builder.append(values[i]);
            } else {
                builder.append(delimiter).append(values[i]);
            }
        }
        return builder.toString();
    }

    public static String join(String delimiter, int[] values) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0, length = values.length; i < length; i++) {
            if (i == 0) {
                builder.append(values[i]);
            } else {
                builder.append(delimiter).append(values[i]);
            }
        }
        return builder.toString();
    }

    public static String join(String delimiter, long[] values) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0, length = values.length; i < length; i++) {
            if (i == 0) {
                builder.append(values[i]);
            } else {
                builder.append(delimiter).append(values[i]);
            }
        }
        return builder.toString();
    }

    public static String join(String delimiter, float[] values) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0, length = values.length; i < length; i++) {
            if (i == 0) {
                builder.append(values[i]);
            } else {
                builder.append(delimiter).append(values[i]);
            }
        }
        return builder.toString();
    }

    public static String join(String delimiter, double[] values) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0, length = values.length; i < length; i++) {
            if (i == 0) {
                builder.append(values[i]);
            } else {
                builder.append(delimiter).append(values[i]);
            }
        }
        return builder.toString();
    }

    public static String join(String delimiter, short[] values) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0, length = values.length; i < length; i++) {
            if (i == 0) {
                builder.append(values[i]);
            } else {
                builder.append(delimiter).append(values[i]);
            }
        }
        return builder.toString();
    }

    public static <E> String join(char delimiter, boolean parse, Collection<E> values) {
        StringBuilder builder = new StringBuilder();
        for(E value: values) {
            if (builder.length() > 0)
                builder.append(delimiter);
            builder.append(parse ? parseObject(value) : value);
        }
        return builder.toString();
    }

    public static <E> String join(char delimiter, Collection<E> values) {
        return join(delimiter, false, values);
    }

    public static <E> String join(String delimiter, boolean parse, Collection<E> values) {
        StringBuilder builder = new StringBuilder();
        for(E value: values) {
            if (builder.length() > 0)
                builder.append(delimiter);
            builder.append(parse ? parseObject(value) : value);
        }
        return builder.toString();
    }

    public static <E> String join(String delimiter, Collection<E> values) {
        return join(delimiter, false, values);
    }

    public static <K, V> String join(char delimiter, boolean parse, Map<K, V> values) {
        StringBuilder builder = new StringBuilder();
        values.forEach((k, v) -> {
            if (builder.length() > 0)
                builder.append(delimiter);
            builder.append(k).append('=').append(parse ? parseObject(v) : v);
        });
        return builder.toString();
    }

    public static <K, V> String join(char delimiter, Map<K, V> values) {
        return join(delimiter, false, values);
    }

    public static <K, V> String join(String delimiter, char equal, boolean parse, Map<K, V> values) {
        StringBuilder builder = new StringBuilder();
        values.forEach((k, v) -> {
            if (builder.length() > 0)
                builder.append(delimiter);
            builder.append(k).append(equal).append(parse ? parseObject(v) : v);
        });
        return builder.toString();
    }

    public static <K, V> String join(String delimiter, boolean parse, Map<K, V> values) {
        return join(delimiter, '=', parse, values);
    }

    public static <K, V> String join(String delimiter, Map<K, V> values) {
        return join(delimiter, false, values);
    }


    public static String lpad(String str, String pad, int num){
        int pad_length = pad.length() * num;
        if (str.length() >= pad_length) {
            return str;
        } else {
            // 此处待优化。即完全用 StringBuilder 实现，可以提升效率。
            char[] pad_chars = pad.toCharArray();
            char[] cb = new char[pad_length - str.length()];
            StringBuilder builder = new StringBuilder();
            for(int i = 0; i < cb.length; i = i + pad_chars.length){
                System.arraycopy(pad_chars, 0, cb, i, pad_chars.length);
                //for (int j = 0; j < pad_chars.length; j++) {
                //    cb[i + j] = pad_chars[j];
                //}
            }
            builder.append(cb).append(str);
            return builder.toString();
        }
    }

    /**
     * 向左补齐
     * @param str 原字符串
     * @param pad 追加字符
     * @param num 字符数量
     * @return 返回补齐后的字符串，如果原字符串长度大于或等于补齐长度，则不进行补齐操作。
     */
    public static String lpad(String str, char pad, int num){
        if (str.length() >= num) {
            return str;
        } else {
            int length = num - str.length();
            StringBuilder builder = new StringBuilder();
            for(int i = 0; i < length; i++){
                builder.append(pad);
            }
            builder.append(str);
            return builder.toString();
        }
    }

    public static int length(String str) {
        if (isEmpty(str)) return 0;
        return str.length();
    }

    /**
     * 解析 Object 对象作为文本输出。
     * @param o 指定对像
     * @param quote 引号符号
     * @return 返回所解析的数据
     */
    public static String parseObject(Object o, char quote) {
        if (o == null) return "NULL";
        StringBuilder builder = new StringBuilder();
        switch( o.getClass().getSimpleName() ){
            case "Char"    : builder.append(quote).append(o).append(quote); break;
            case "Integer" : builder.append(o); break;
            case "Long"    : builder.append(o); break;
            case "String"  : builder.append(quote).append(o).append(quote); break;
            case "Double"  : builder.append(o); break;
            case "Float"   : builder.append(o); break;
            case "Date"    : builder.append(quote).append(o).append(quote); break;
            default: builder.append(quote).append(o).append(quote); break;
        }
        return builder.toString();
    }

    /**
     * 解析 Object 对象作为文本输出。
     * @param o 指定对像
     * @return 返回所解析的数据
     */
    public static String parseObject(Object o) {
        return parseObject(o, '\'');
    }

    /**
     * 向右补齐
     * @param str 原字符串
     * @param pad 追加字符
     * @param num 字符数量
     * @return 返回补齐后的字符串，如果原字符串长度大于或等于字符数量，则不进行补齐操作。
     */
    public static String rpad(String str, char pad, int num){
        if (str.length() >= num) {
            return str;
        } else {
            int length = num - str.length();
            StringBuilder builder = new StringBuilder();
            builder.append(str);
            for(int i = 0; i < length; i++){
                builder.append(pad);
            }
            return builder.toString();
        }
    }

    public static String rpad(String str, String pad, int num){
        int pad_length = pad.length() * num;
        if (str.length() >= pad_length) {
            return str;
        } else {
            // 此处待优化。即完全用 StringBuilder 实现，可以提升效率。
            char[] pad_chars = pad.toCharArray();
            char[] cb = new char[pad_length - str.length()];
            StringBuilder builder = new StringBuilder();
            for(int i = 0; i < cb.length; i = i + pad_chars.length){
                System.arraycopy(pad_chars, 0, cb, i, pad_chars.length);
                //for (int j = 0; j < pad_chars.length; j++) {
                //    cb[i + j] = pad_chars[j];
                //}
            }
            builder.append(str).append(cb);
            return builder.toString();
        }
    }

    /**
     * 在字符串数组中查询指定的字符串。
     * @param source 指定字符串数组
     * @param target 要查找的字符串
     * @return 返回字符串索引。如果未查找到，则返回 -1。
     */
    public static int search(String[] source, String target) {
        if (source == null)
            return -1;
        for (int i = 0, length = source.length; i < length; i++) {
            if (equals(source[i], target))
                return i;
        }
        return -1;
    }



    public static String[] split(String str, char delimiter, int limit) {
        if (isEmpty(str))
            return null;
        int next = 0, pos, length = str.length();
        String[] retval = new String[limit];
        for(int i = 0; i < limit; i++) {
            if ((pos = str.indexOf(delimiter, next)) != -1) {
                retval[i] = str.substring(next, pos);
                next = ++pos;
            } else {
                if (next < length) {
                    retval[i] = str.substring(next);
                    next = length;
                }
            }
        }
        return retval;
    }

    public static String[] split(String str, String delimiter, int limit) {
        if (isEmpty(str))
            return null;
        int next = 0, pos, length = str.length();
        String[] retval = new String[limit];
        for(int i = 0; i < limit; i++) {
            if ((pos = str.indexOf(delimiter, next)) != -1) {
                retval[i] = str.substring(next, pos);
                next = ++pos;
            } else {
                if (next < length) {
                    retval[i] = str.substring(next);
                    next = length;
                }
            }
        }
        return retval;
    }

    public static String[] split(String str, char delimiter) {
        if (isEmpty(str)) return null;
        int next = 0, pos, fields = count(str, delimiter), length = str.length();
        String[] retval = new String[fields];
        for(int i = 0; i < fields; i++) {
            if ((pos = str.indexOf(delimiter, next)) != -1) {
                retval[i] = str.substring(next, pos);
                next = ++pos;
            } else {
                if (next < length) {
                    retval[i] = str.substring(next);
                    next = length;
                } else {
                    retval[i] = "";
                }

            }
        }
        return retval;
    }

    public static String[] split(String str, String delimiter) {
        if (isEmpty(str)) return null;
        // 在字有一个字符的情况下，直接使用 char 来分隔，更具有效率。
        if (str.length() == 1)
            return split(str, str.charAt(0));
        int next = 0, pos, fields = count(str, delimiter), length = str.length();
        String[] retval = new String[++fields];
        for(int i = 0; i < fields; i++) {
            if ((pos = str.indexOf(delimiter, next)) != -1) {
                retval[i] = str.substring(next, pos);
                next = ++pos;
            } else {
                if (next < length) {
                    retval[i] = str.substring(next);
                    next = length;
                } else {
                    retval[i] = "";
                }

            }
        }
        return retval;
    }

    public static String[] split(String str, String delimiter, String[] buffer) {
        if (isEmpty(str)) return null;
        int next = 0, pos, fields, length = str.length();
        if (buffer == null) {
            fields = count(str, delimiter);
            buffer = new String[fields];
        } else {
            fields = buffer.length;
        }
        for(int i = 0; i < fields; i++) {
            if ((pos = str.indexOf(delimiter, next)) != -1) {
                buffer[i] = str.substring(next, pos++);
                next = pos;
            } else {
                if (next < length) {
                    buffer[i] = str.substring(next);
                    next = length;
                } else {
                    buffer[i] = "";
                }

            }
        }
        return buffer;
    }

    public static String vaild(String str, String defaultValue) {
        return isEmpty(str) ?  defaultValue : str;
    }

    /**
     * 是否数字
     * @param str 指定字符串
     * @return 返回 true 表示该字符串是数字，返回 false 表示该字符串不是数字。
     */
    public static boolean isNumeric(String str) {
        if (isEmpty(str))
            return false;
        int  length    = str.length();
        char firstChar = str.charAt(0);
        if (firstChar >= '0' && firstChar <= '9') {
            return true;
        } else {
            char secondChar = length >= 2 ? str.charAt(1) : '\0';
            return (firstChar == '-' || firstChar == '+')
                    && ( secondChar >= '0' && str.charAt(1) <= '9');
        }
    }

}
