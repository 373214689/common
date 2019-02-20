package com.liuyang.jdbc.mysql;

import com.liuyang.ds.Row;
import com.liuyang.ds.sets.DataRow;
import com.liuyang.jdbc.Column;
import com.liuyang.jdbc.Database;
import com.liuyang.jdbc.Table;
import com.liuyang.tools.StringUtils;
import com.sun.istack.internal.NotNull;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

public final class MySQLTable implements Table, Cloneable {

    private MySQLDataBase database;
    private String name;
    private String location;

    private LinkedHashMap<String, Column> fields;

    public MySQLTable(@NotNull MySQLDataBase database, @NotNull String name) {
        this.database = database;
        this.name     = name;
        this.fields   = new LinkedHashMap<>();
    }

    @Override
    protected void finalize() {
        if (fields != null)
            fields.clear();
        database = null;
        name     = null;
        location = null;
        fields   = null;
    }

    /**
     * 添加字段
     * @param column 字段
     * @return 返回 true，表示添加成功，返回 false 表示添加失败（一般是因为字段已存在）。
     */
    public boolean addField(Column column) {
        return fields.put(column.getName(), column) == null;
    }

    public boolean create(MySQLManager manager) throws MySQLException{
        if (manager.isConnected()) {
            return manager.execute(showCreateTable());
        }
        return false;
    }

    public boolean create(MySQLConfig conf) throws MySQLException {
        try (MySQLManager manager = conf.getConnection()) {
            return manager.execute(showCreateTable());
        }
    }

    /**
     * 复制为新的数据表。新的数据表不会现原数据表共用字段列和表名。
     * @return 返回新的数据表。
     */
    @Override
    public MySQLTable clone() {
        MySQLTable table = new MySQLTable(database, name);
        for(String key : fields.keySet())
            table.fields.put(key, fields.get(key).clone());
        table.location  = location;
        return table;
    }

    public boolean exists(MySQLManager manager) throws MySQLException {
        if (manager.isConnected()) {
            return manager.existsTable(database.getName(), name);
        }
        return false;
    }

    public boolean exists(MySQLConfig conf) throws MySQLException {
        try (MySQLManager manager = conf.getConnection()){
            return manager.existsTable(database.getName(), name);
        }
    }

    public boolean createTable(@NotNull MySQLConfig conf) throws MySQLException {
        try (MySQLManager manager = conf.getConnection()) {
            return manager.execute(showCreateTable());
        }
    }

    public Database getDatabase() {
        return database;
    }

    public String getFullName() {
        return database.getName() + "." + name;
    }

    public String getName() {
        return name;
    }

    public String getLocation() {
        return location;
    }

    /**
     * 获取字段数组
     * @return  返回字段数组
     */
    public Column[] getFields() {
        return fields.values().stream().toArray(Column[]::new);
    }

    public Column getField(String name) {
        Column column = fields.get(name);
        if (column == null)
            throw new IllegalArgumentException("The field named \"" + name + "\" can't be matched .");
        return column;
    }

    /**
     * 插入字段
     * @param name 指定字段名称
     * @param after 是否在指定字段后面插入，true 在此之后插入，false 表示在此之前插入。
     * @param column 将要插入的字段数据
     * @return 返回 true，表示删除成功，返回 false 表示删除失败（一般是因为字段不存在）。
     */
    public boolean insertField(String name, boolean after, Column column) {
        if (fields.containsKey(name)) {
            LinkedHashMap<String, Column> map = new LinkedHashMap<>();
            Iterator<Map.Entry<String, Column>> itor = fields.entrySet().iterator();
            while(itor.hasNext()) {
                Map.Entry<String, Column> element = itor.next();
                String key = element.getKey();
                Column value = element.getValue();
                if (key.equals(name)) {
                    if (after) {
                        map.put(key, value);
                        map.put(column.getName(), column);
                    } else {
                        map.put(column.getName(), column);
                        map.put(key, value);
                    }
                } else {
                    map.put(key, value);
                }
                itor.remove();
            }
            fields = map;
            return true;
        } else {
            return false;
        }
    }

    /**
     * 插入数据
     * @param conf 指定 MySQL 配置信息
     * @param parameters 需要插入的数据。该数据的数组长度需要与表字段保持一致。
     * @return 返回插入结果数量。返回 -1 表示插入中出现异常或没有插入成功。
     * @throws MySQLException 如果执行过程中出错误，则抛出该异常。
     */
    public synchronized int insertValues(@NotNull MySQLConfig conf,
                                         @NotNull Object[]... parameters) throws MySQLException {
        try (MySQLManager manager = conf.getConnection()) {
            if (!manager.existsDatabase(database.getName()))
                manager.execute(database.showCreateDatabase());
            if (!manager.existsTable(database.getName(), name))
                manager.execute(showCreateTable());
            String[] fieldNames = fields.values().stream().map(Column::getName).toArray(String[]::new);
            return manager.batchInsert(database.getName(), name, 10000, true, fieldNames, parameters);
        }
    }

    public synchronized int mergeValue(@NotNull MySQLConfig conf,
                                       @NotNull Map<String, Object> values) throws MySQLException {
        try (MySQLManager manager = conf.getConnection()) {
            if (!manager.existsDatabase(database.getName()))
                manager.execute(database.showCreateDatabase());
            if (!manager.existsTable(database.getName(), name))
                manager.execute(showCreateTable());
            String[] fieldNames = fields.values().stream()
                    .filter(Column::isPrimary).map(Column::getName).toArray(String[]::new);
            return manager.merge(database.getName(), name, fieldNames, values);
        }
    }

    public synchronized int megreValues(@NotNull MySQLConfig conf,
                                        @NotNull Map<String, Object>... parameters) throws MySQLException {
        try (MySQLManager manager = conf.getConnection()) {
            if (!manager.existsDatabase(database.getName()))
                manager.execute(database.showCreateDatabase());
            if (!manager.existsTable(database.getName(), name))
                manager.execute(showCreateTable());
            String[] fieldNames = fields.values().stream()
                    .filter(Column::isPrimary).map(Column::getName).toArray(String[]::new);
            return manager.batchMerge(database.getName(), name, fieldNames, parameters);
        }
    }

    /**
     * 打印建表语句
     */
    public void printlnCreateTable() {
        System.out.println(showCreateTable());
    }

    /**
     * 删除字段
     * @param column 指定字段
     * @return 返回 true，表示删除成功，返回 false 表示删除失败（一般是因为字段不存在）。
     */
    public boolean removeField(Column column) {
        return fields.remove(column.getName(), column);
    }

    /**
     * 删除字段
     * @param name 指定字段名称
     * @return 返回 true，表示删除成功，返回 false 表示删除失败（一般是因为字段不存在）。
     */
    public boolean removeField(String name) {
        return fields.remove(name) != null;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    /**
     * 设置名称
     * @param name 指定名称
     * @return 返回当前实例
     */
    public MySQLTable setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * 设置名称
     * @param action 传入原本名称，返回修改后的名称
     * @return 返回当前实例
     */
    public MySQLTable setName(Function<String, String> action) {
        String changed = action.apply(name);
        if (!StringUtils.isEmpty(changed))
            this.name = changed;
        return this;
    }

    /**
     * 设置主键
     * @param fieldNames 指定字段名称。多个名称之间可以使用逗号(",")分隔，也可以单独作为参数传递。
     */
    public MySQLTable setPrimaryKeys(String... fieldNames) {
        if (fieldNames == null)
            return this;
        if (fieldNames.length <= 0)
            return this;
        for(int i = 0, length = fieldNames.length; i < length; i++) {
            if (!StringUtils.isEmpty(fieldNames[i])) {
                String[] names = fieldNames[i].split(",");
                for (String name : names) {
                    Column column = fields.get(name.trim());
                    if (column != null) {
                        column.setPrimary(true);
                    } else {
                        throw new IllegalArgumentException("Can not matched field with name of \"" + name + "\"." );
                    }
                }
            }
        }
        return this;
    }

    /**
     * 生成创建表的 SQL 语句。
     * @return 返回建表 SQL 语句。
     */
    public String showCreateTable() {
        StringBuilder builder = new StringBuilder();
        builder.append("create table if not exists ");
        builder.append(database.getName()).append('.').append(name);
        builder.append(" (\n");
        int max = fields.values().stream().mapToInt(e -> e.getName().length()).max().getAsInt();
        Column[] columns = fields.values().stream().toArray(Column[]::new);
        String[] primary = fields.values().stream().filter(Column::isPrimary)
                .map(Column::getName).toArray(String[]::new);
        for (int i = 0, length = columns.length; i < length; i++) {
            Column column = columns[i];
            builder.append(' ').append(' ');
            // 需要给字段加上上标("`")标识。
            builder.append(StringUtils.rpad("`" + column.getName() + "`", ' ', max + 2));
            builder.append(' ').append(column.getType().getName());
            int precision = column.getPrecision();
            int scale = column.getScale();
            builder.append('(');
            switch(column.getType()) {
                case DOUBLE: {
                    if (precision > 0)
                        builder.append(scale == 0 ? 11 : scale).append(", ").append(precision);
                    break;
                }
                case FLOAT: {
                    if (precision > 0)
                        builder.append(scale == 0 ? 11 : scale).append(", ").append(precision);
                    break;
                }
                case INT:
                    if (scale > 0)
                        builder.append(scale);
                    break;
                case BIGINT:
                    if (scale > 0)
                        builder.append(scale);
                    break;
                case SMALLINT:
                    if (scale > 0)
                        builder.append(scale);
                    break;
                case VARCHAR: {
                    if (scale > 0)
                        builder.append(scale);
                    break;
                }
                default:
                    break;
            }
            builder.append(')');
            if (column.isPrimary() || !column.isNullable())
                builder.append(" not null");
            if (primary.length > 0) {
                builder.append(',');
            } else {
                if ((i + 1) < length)
                    builder.append(',');
            }
            builder.append('\n');
        }
        if (primary.length > 0) {
            builder.append(' ').append(' ');
            builder.append("primary key (`").append(String.join("`, `", primary)).append("`)\n");
        }
        builder.append(')');
        return builder.toString();
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        fields.forEach((k, v) -> {
            map.put(k, null);
        });
        return map;
    }

    @Override
    public Row toRow() {
        return new DataRow(fields.values().stream().toArray(Column[]::new));
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(database.getName()).append('.').append(name).append('(');
        String[] columns = fields.values().stream().map(Object::toString).toArray(String[]::new);
        builder.append(String.join(", ", columns));
        builder.append(')');
        return builder.toString();
    }

    /**
     * 更新字段
     * @param column 指定字段
     * @return 返回 true，表示更新成功，返回 false 表示更新失败（一般是因为字段不存在）。
     */
    public boolean updateField(Column column) {
        Column old = fields.get(column.getName());
        if (old != null)
            return fields.replace(column.getName(), old, column);
        else
            return false;
    }

}
