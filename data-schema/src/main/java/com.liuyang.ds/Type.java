package com.liuyang.ds;

import java.sql.Types;

/**
 * Schema Type Defined
 * <ul>
 * Use one of follow
 * <li> {@code BINARY} </li>
 * <li> {@code BOOLEAN} </li>
 * <li> {@code DOUBLE} </li>
 * <li> {@code FLOAT} </li>
 * <li> {@code INT} </li>
 * <li> {@code LONG} </li>
 * <li> {@code SHORT} </li>
 * <li> {@code STRING} </li>
 * <li> {@code STRUCT} </li>
 * </ul>
 * @version 1.0.2
 * @author liuyang
 *
 */
public enum Type {
    BYTE(Types.BIT, "byte", true),
    BINARY(Types.BINARY, "binary", true),
    BYTEARRAY(Types.BINARY, "bytearray", true),
    BOOLEAN(Types.BOOLEAN, "boolean", true),
    BOOL(Types.BOOLEAN, "bool", true),

    CHAR(Types.CHAR, "char", true),
    DECIMAL(Types.DECIMAL, "decimal", true),
    DOUBLE(Types.DOUBLE, "double", true),
    FLOAT(Types.FLOAT, "float", true),
    /** 字段*/
    TINYINT(Types.TINYINT, "tinyint", true),
    /** 整数 */
    INT(Types.INTEGER, "int", true),
    INTEGER(Types.INTEGER, "integer", true),
    LIST(Types.OTHER, "list", false),
    /** 长整数 */
    BIGINT(Types.BIGINT, "bigint", true),
    LONG(Types.BIGINT, "long", true),
    MAP(Types.OTHER, "map", false),
    PRIMITVE(Types.OTHER, "primitve", true),
    SMALLINT(Types.SMALLINT, "smallint", true),
    STRUCT(Types.STRUCT, "struct", false),
    /**短整数: short, smallint, 取值范围: -32768 - 32768*/
    SHORT(Types.SMALLINT, "short", true),
    /**字符串: string, str*/
    STRING(Types.VARCHAR, "string", true),
    TEXT(Types.VARCHAR, "text", true),
    UNION(Types.OTHER, "union", false),
    VARCHAR(Types.VARCHAR, "varchar", true),
    OBJECT(Types.JAVA_OBJECT, "object", false);

    /**
     * Try find the type of name
     * @param name the type name, as {@code String}
     * @return the type {@link Type}
     * @exception IllegalArgumentException if the type of name undefined.
     */
    public static Type lookup(String name) {
        String lowerName = name.toLowerCase();
        for(Type value: values()) {
            if (value.name.equals(lowerName)) return value;
        }
        throw new IllegalArgumentException("Illegal parameter [type name = " + lowerName + "], type is undefined.");
    }

    public static Type lookup(int typeId) {
        switch(typeId) {
            case Types.BIGINT      : return Type.BIGINT;
            case Types.BOOLEAN     : return Type.BOOLEAN;
            case Types.BINARY      : return Type.BINARY;
            case Types.CHAR        : return Type.CHAR;
            case Types.REAL        : return Type.DOUBLE;
            case Types.DOUBLE      : return Type.DOUBLE;
            case Types.FLOAT       : return Type.FLOAT;
            case Types.INTEGER     : return Type.INT;
            case Types.NUMERIC     : return Type.DOUBLE;
            case Types.VARCHAR     : return Type.VARCHAR;
            case Types.SMALLINT    : return Type.SMALLINT;
            case Types.LONGVARCHAR : return Type.STRING;
            default:
                throw new IllegalArgumentException("Illegal parameter [typeId = " + typeId + "], type is undefined.");
        }
    }

    /**
     * 判断是否属性数字类型
     * @param type 指定类型
     * @return 返回 true 表示是数字类型，返回 false 表示非数字类型。
     */
    public static boolean isNumeric(Type type) {
        switch (type) {
            case BYTE:     return true;
            case DOUBLE:   return true;
            case FLOAT:    return true;
            case INT:      return true;
            case INTEGER:  return true;
            case LONG:     return true;
            case BIGINT:   return true;
            case SHORT:    return true;
            case SMALLINT: return true;
            case TINYINT:  return true;
            default:       return false;
        }
    }

    /**
     * 判断是否布尔类型
     * @param type 指定类型
     * @return 返回 true 表示是布尔类型，返回 false 表示非布尔类型。
     */
    public static boolean isBoolean(Type type) {
        switch (type) {
            case BOOL:     return true;
            case BOOLEAN:   return true;
            default:       return false;
        }
    }

    private int     id;
    private String  name;
    private boolean isPrimitive;

    Type(int id, String name, boolean isPrimitive) {
        this.id           = id;
        this.name         = name;
        this.isPrimitive  = isPrimitive;
    }

    /**
     * 获取名称
     * @return 返回名称。
     */
    public String getName() {
        return name;
    }

    /**
     * 获取标识
     * @return 返回标识。
     */
    public int getId() {
        return id;
    }

    /**
     * 是否原始类型
     * @return 返回 true 表示为原始类型， 返回 false 表示不是。
     */
    public boolean isPrimitive() {
        return isPrimitive;
    }

    @Override
    public String toString() {
        return "{name: " + name + ", isPrimitive: " + isPrimitive + "}";
    }

    /*private IllegalArgumentException exception(Object value, String type) {
        return new IllegalArgumentException(
                "Illegal paramater <" + value.getClass() + ">: " + value + ", type is not " + type + ".");
    }*/
}
