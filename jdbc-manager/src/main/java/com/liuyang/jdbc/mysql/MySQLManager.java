package com.liuyang.jdbc.mysql;

import com.liuyang.common.ManagerException;
import com.liuyang.ds.Node;
import com.liuyang.ds.Row;
import com.liuyang.ds.Schema;
import com.liuyang.jdbc.AbstractManager;
import com.liuyang.jdbc.Column;
import com.liuyang.tools.StringUtils;
import com.sun.istack.internal.NotNull;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * MySQL 管理器
 *
 * @author liuyang
 * @version 1.0.1
 */
public class MySQLManager extends AbstractManager {

    MySQLManager() { }

    public MySQLManager(@NotNull MySQLConfig config) {
        super.conf = config;
    }

    private String createMergeStatement(String database, String tableName,
                                        String[] primaryKeys, Map<String, Object> values) {
        StringBuilder builder = new StringBuilder();
        Map<String, Object> merged = new LinkedHashMap<>();
        // 复制除主键之外的数据
        values.entrySet().stream().filter(e -> StringUtils.search(primaryKeys, e.getKey()) > 0)
                .forEach(e -> merged.put(e.getKey(), e.getValue()));
        // 建立SQL语句
        builder.append("insert into ").append(database).append('.').append(tableName);
        builder.append(" (`").append(StringUtils.join("`, `", false, values.keySet())).append("`)");
        builder.append(" values(").append(StringUtils.join(", ", true, values.values())).append(')');
        builder.append(" on duplicate key update ").append(StringUtils.join(", ", true, merged));
        return builder.toString();
    }
    @Override
    public synchronized int batchMerge(String database, String tableName, String[] primaryKeys,
                                       Map<String, Object>... parameters) throws MySQLException {
        if (parameters == null)
            return -1;
        requireConnection();
        requireConnected();
        String[] inSqlStr = new String[parameters.length];
        for (int i = 0, size = parameters.length; i < size; i++) {
            inSqlStr[i] = createMergeStatement(database,tableName, primaryKeys, parameters[i]);
        }
        int result = 0;
        for (int x : batchExecute(inSqlStr))
            result += x;
        return result;
    }

    /**
     * 连接。
     * @param conf 指定 MySQL 配置。
     * @return 返回 true 表示连接成功，返回 false 表示连接失败。
     * @throws MySQLException 执行过程中出错则抛出该异常。
     */
    public synchronized boolean connect(MySQLConfig conf) throws MySQLException {
        return super.connect(conf);
    }

    @Override
    public synchronized boolean connect(String hostname, int port, String defaultDatabase,
                                        String username, String password) throws MySQLException {
        MySQLConfig conf = new MySQLConfig(hostname, port, username, password);
        conf.setDatabase(defaultDatabase);
        return super.connect(conf);
    }

    /**
     * 使用指定的 host、port、user、pass 连接数据库。
     * @param hostname 指定服务器主机
     * @param port     指定服务器端口
     * @param username 指定用户名称
     * @param password 指定用户密码
     * @return 返回 true 表示连接成功，返回 false 表示连接失败。
     * @throws MySQLException 执行过程中出错，则抛出该异常。
     */
    public synchronized boolean connect(String hostname, int port,
                                        String username, String password) throws MySQLException {
        return this.connect(hostname, port, "mysql", username, password);
    }

    @Override
    public synchronized boolean connect(String url, String user, String pass) throws MySQLException {
        MySQLConfig conf = new MySQLConfig();
        try {
            conf.parseURI(new URI(url));
            conf.setUser(user);
            conf.setPass(pass);
            return super.connect(conf);
        } catch (URISyntaxException e) {
            throw new MySQLException("Syntax Error: Can not parse url(" + url + "), " + e.getMessage());
        } catch (UnsupportedEncodingException e) {
            throw new MySQLException("Encoding Error: Can not parse url(" + url + "), " + e.getMessage());
        }
    }

    @Override
    public synchronized boolean existsDatabase(String database) throws MySQLException {
        if (StringUtils.isEmpty(database))
            return false;
        requireConnection();
        requireConnected();
        // 获取数据库
        try (ResultSet rs = conn.getMetaData().getCatalogs()){
            // 记录最后连接时间
            recordLastConnectionTime();
            while (rs.next()) {
                if (database.equals(rs.getString("TABLE_CAT")))
                    return true;
            }
        } catch (SQLException e) {
            throw new MySQLException(e.getMessage(), e);
        }
        return false;
    }

    @Override
    public synchronized boolean existsTable(String database, String tableName) throws MySQLException {
        if (StringUtils.isEmpty(tableName))
            return false;
        requireConnection();
        requireConnected();
        // 获取表
        try (ResultSet rs = conn.getMetaData().getTables(database, null, tableName, new String[]{"TABLE"})) {
            // 记录最后连接时间
            recordLastConnectionTime();
            return rs.next();
        } catch (SQLException e) {
            throw new MySQLException(e.getMessage(), e);
        }
    }

    /**
     * 检测是否存在数据表。
     * @param tableName 指定数据表名称。可以使用数据库名徐做为前缀。
     * @return 返回 true 表示存在，返回 false 表示不存在。
     * @throws MySQLException 执行过程中出错，则抛出该异常。
     */
    public synchronized boolean existsTable(String tableName) throws MySQLException {
        String database = null;
        String name = tableName;
        int pos = tableName.indexOf('.');
        if (pos > 0) {
            database = tableName.substring(0, pos);
            name = tableName.substring(pos + 1);
        }
        return existsTable(database, name);
    }

    @Override
    public synchronized List<MySQLDataBase> getDatabases() throws MySQLException {
        requireConnection();
        requireConnected();
        List<MySQLDataBase> list = new LinkedList<>();
        // 获取数据库
        try (ResultSet rs = conn.getMetaData().getCatalogs()){
            while(rs.next()) {
                // 记录最后连接时间
                recordLastConnectionTime();
                list.add(new MySQLDataBase(rs.getString("TABLE_CAT"), ""));
            }
        } catch (SQLException e) {
            throw new MySQLException(e.getMessage(), e);
        }
        return list;
    }

    @Override
    public synchronized MySQLTable getTable(String database, String tableName) throws MySQLException {
        requireConnection();
        requireConnected();
        MySQLTable table = null;
        // 获取字段
        try (ResultSet rs = conn.getMetaData().getColumns(database, null, tableName, null)){
            while (rs.next()) {
                // 记录最后连接时间
                recordLastConnectionTime();
                if (table == null) {
                    String user = conn.getMetaData().getUserName();
                    table = new MySQLTable(new MySQLDataBase(database, user), tableName);
                }
                Column column = new Column(rs.getString("COLUMN_NAME"),
                        rs.getString("COLUMN_NAME"),
                        rs.getInt("COLUMN_NAME"),
                        rs.getInt("COLUMN_NAME"));
                column.setNullable(rs.getInt("NULLABLE") == 0);
                table.addField(column);
            }
        } catch (SQLException e) {
            throw new MySQLException(e.getMessage(), e);
        }
        return table;
    }

    public synchronized MySQLTable getTable(String tableName) throws MySQLException {
        requireConnection();
        requireConnected();
        MySQLTable table = null;
        // 解析表名
        int pos = tableName.indexOf('.');
        String database = pos > 0 ? tableName.substring(0, pos) : conf.getDatabase();
        String name = pos > 0 ? tableName.substring(pos + 1) : tableName;
        try (ResultSet rs = conn.getMetaData().getColumns(database, null, name, null)) {
            String user = conn.getMetaData().getUserName();
            // 获取字段
            while (rs.next()) {
                // 记录最后连接时间
                recordLastConnectionTime();
                if (table == null) {
                    table = new MySQLTable(new MySQLDataBase(database, user), name);
                }
                Column column = new Column(rs.getString("COLUMN_NAME"),
                        rs.getString("COLUMN_NAME"),
                        rs.getInt("COLUMN_NAME"),
                        rs.getInt("COLUMN_NAME"));
                column.setNullable(rs.getInt("NULLABLE") == 0);
                table.addField(column);
            }
        } catch (SQLException e) {
            throw new MySQLException(e.getMessage(), e);
        }
        return table;
    }

    @Override
    public synchronized List<MySQLTable> getTables(String database) throws MySQLException {
        requireConnection();
        requireConnected();
        List<MySQLTable> tables = new ArrayList<>();
        try (ResultSet rs = conn.getMetaData().getTables(database, null, "%", new String[]{"TABLE"})) {
            while (rs.next()) {
                String user = conn.getMetaData().getUserName();
                String dbName = rs.getString("TABLE_CAT");
                tables.add(new MySQLTable(new MySQLDataBase(dbName, user), rs.getString("TABLE_NAME")));
            }
        } catch (SQLException e) {
            throw new MySQLException(e.getMessage(), e);
        }
        return tables;
    }

    @Override
    public synchronized final int merge(String database, String tableName,
                                        String[] primaryKeys, Map<String, Object> values) throws MySQLException {
        if (values == null)
            return -1;
        if (values.size() <= 0)
            return -1;
        requireConnection();
        requireConnected();
        StringBuilder builder = new StringBuilder();
        Map<String, Object> merged = new LinkedHashMap<>();
        //Arrays.binarySearch(primaryKeys, "", String::compareTo);
        // 复制除主键之外的数据
        values.forEach((k, v) -> {
            if (!StringUtils.contains(primaryKeys, k))
                merged.put(StringUtils.builder('`').append(k).append('`').toString(), v);
        });
        //values.entrySet().stream().filter(e -> !StringUtils.contains(primaryKeys, e.getKey()))
        //        .forEach(e -> {
        //            merged.put(StringUtils.builder('`').append(e.getKey()).append('`').toString(), e.getValue());
        //        });
        // 建立SQL语句
        builder.append("insert into ").append(database).append('.').append(tableName);
        builder.append(" (`").append(StringUtils.join("`, `", false, values.keySet())).append("`)");
        builder.append(" values(").append(StringUtils.join(", ", true, values.values())).append(')');
        builder.append(" on duplicate key update ").append(StringUtils.join(", ", true, merged));
        return update(builder.toString());
    }

    @Override
    public synchronized int merge(String database, String tableName, Row values) throws ManagerException {
        if (values == null)
            return -1;
        // 取主键
        String [] primaryKeys = Arrays.stream(values.header(true)).map(Schema::getName).toArray(String[]::new);
        // 组织数据
        Map<String, Object> map = values.toNamedMap();
        // 合并
        return merge(database, tableName, primaryKeys, map);
    }

    // 测试期间的功能，建议发布时删除
    @Deprecated
    public synchronized void printTable(String database, String tableName) throws MySQLException {
        requireConnection();
        requireConnected();
        // 获取字段
        try (ResultSet rs = conn.getMetaData().getColumns(database, null, tableName, null)) {
            int lines = 0;
            while (rs.next()) {
                // 记录最后连接时间
                recordLastConnectionTime();
                ArrayList<String> row;
                int length = rs.getMetaData().getColumnCount();
                if (lines == 0) {
                    row = new ArrayList<>();
                    for (int i = 1; i < length; i++) {
                        row.add(String.valueOf(rs.getMetaData().getColumnLabel(i)));
                    }
                    System.out.println(row);
                }

                row = new ArrayList<>();
                for (int i = 1; i < length; i++) {
                    row.add(String.valueOf(rs.getObject(i)));
                }
                System.out.println(row);
                lines++;
            }
        } catch (SQLException e) {
            throw new MySQLException(e.getMessage(), e);
        }
    }

    public synchronized Map<String, String> getVariables() throws MySQLException {
        List<Row> list = super.query("show global variables").toList();
        Map<String, String> retval = new HashMap<>();
        list.stream()
                //.map(e -> new Node<>(e.getString("Variable_name"), e.getString("Value")))
                .map(e -> Node.create(e.getString("Variable_name"), e.getString("Value")))
                .forEach(e -> retval.put(e.getKey(), e.getValue()));
        list.clear();
        return retval;
    }

}
