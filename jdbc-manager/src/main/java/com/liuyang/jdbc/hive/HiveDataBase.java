package com.liuyang.jdbc.hive;

import com.liuyang.jdbc.Database;
import com.liuyang.jdbc.mysql.MySQLTable;

import java.util.HashMap;
import java.util.Map;

/**
 * Hive 数据库
 */
public class HiveDataBase implements Database {

    private String name;
    private String owner;

    private Map<String, MySQLTable> tables;

    public HiveDataBase(String name, String owner) {
        this.name = name;
        this.owner = owner;
        this.tables = new HashMap<>();
    }

    public String getName() {
        return name;
    }

    public String getOwner() {
        return owner;
    }

    /**
     * 添加表
     * @param table 指定的表
     * @return 返回 true 表示添加成功，返回 false 表示存在同名表或未添加成功。
     */
    public boolean addTable(MySQLTable table) {
        if (table == null)
            return false;
        if (tables.containsKey(table.getName()))
            return false;
        return tables.put(table.getName(), table) == null;
    }

    public MySQLTable[] getTables() {

        return tables.values().stream().toArray(MySQLTable[]::new);
    }

    public boolean removeTable(String tableName) {
        return tables.remove(tableName) != null;
    }

    public String showCreateDatabase() {
        StringBuilder builder = new StringBuilder();
        builder.append("create database if not exists ").append(name);
        return builder.toString();
    }
}
