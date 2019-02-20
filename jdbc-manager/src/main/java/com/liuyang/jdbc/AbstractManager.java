package com.liuyang.jdbc;

import com.liuyang.common.ManagerClient;
import com.liuyang.common.ManagerConfig;
import com.liuyang.common.ManagerException;
import com.liuyang.ds.Row;
import com.liuyang.ds.Schema;
import com.liuyang.ds.Type;
import com.liuyang.ds.sets.DataRow;
import com.liuyang.jdbc.mysql.MySQLException;
import com.liuyang.tools.StringUtils;

import java.sql.*;
import java.util.*;


/**
 * Abstract JDBC Manager
 * <ul>
 *     <li>2019/1/1 ver 1.0.0 LiuYang Created,</li>
 *     <li>2019/2/1 ver 1.0.1 LiuYang Add function: merge,</li>
 * </ul>
 *
 * @author liuyang
 * @version 1.0.1
 */
public abstract class AbstractManager implements ManagerClient, AutoCloseable {

    protected volatile Connection         conn;
    protected volatile AbstractJDBCConfig conf;
    private   volatile long               last;
    //private   String         name;

    protected AbstractManager() {

    }

    /**
     * 检测连接是否初始化
     * @throws ManagerException 如果未初始化，则抛出异常。
     */
    protected synchronized final void requireConnection() throws ManagerException {
        if (conn == null)
            throw new ManagerException("need a valid connection.");
    }

    /**
     * 连接数据库是否已连接
     * @throws ManagerException 如果未连接，则抛出异常。
     */
    protected synchronized final void requireConnected() throws ManagerException {
        if (!isConnected()) {
            throw new ManagerException("not connected.");
        }
        recordLastConnectionTime();
    }

    /**
     * 连接到数据库。
     * <p>
     *     <i>调用此功能之前，需要初始化连接配置。如果初始化，则可以直接使用该功能。</i>
     * </p>
     * @return 返回 true 表示连接成功， 返回 false 表示连接失败。
     * @throws ManagerException 如果调用 <code>Driver.getConnection</code> 失败，则抛出该异常。
     */
    public synchronized final boolean connect() throws ManagerException {
        if (conf == null)
             throw new ManagerException("The config is not initialized. Please initialize it at first. ");
        return connect(conf);
    }

    /**
     * 使用指定的配置连接到数据库
     * @param conf 指定配置
     * @return 返回 true 表示连接成功， 返回 false 表示连接失败。
     * @throws ManagerException 如果配置或网络有问题，则抛出相应的异常。
     */
    protected synchronized final boolean connect(AbstractJDBCConfig conf) throws ManagerException{
        boolean successful;
        if (conf == null)
            return false;
        try {
            Class.forName(conf.getDriverName());
        } catch (ClassNotFoundException e) {
            throw new ManagerException("Driver class (" + conf.getDriverName() + ") not found.", e);
        }
        Connection conn;
        try {
            conn = DriverManager.getConnection(conf.toString(), conf.getUser(), conf.getPass());
            if (conn == null)
                return false;
            successful = !conn.isClosed();
        } catch (SQLException e) {
            throw new ManagerException("Can not connect to server, URI: " +
                    conf.toString() + ". " + e.getMessage());
        }
        // 连接后验证连接是否有效
        recordLastConnectionTime();
        /*try {
            succ = conn.isValid(3000);
        } catch (SQLException e) {
            throw new ManagerException("Connect to server timeout, URI: " +
                    conf.toString() + ". " + e.getMessage());
        } finally {
        if (succ) {
            this.conn = conn;
            this.conf = conf;

        }
        }*/
        this.conn = conn;
        this.conf = conf;
        return successful;
    }

    /**
     * 使用指定的URL连接数据库
     * <p>
     *     (***推荐***)
     *     注意：URL 中的 Schema 要与 Manager 中的一致，否则会报错。
     * </p>
     * @param url URL，如：jdbc:mysql://hostname:port/database?query
     * @param user 用户名称
     * @param pass 密码
     * @return 返回 true 表示连接成功， 返回 false 表示连接失败。
     * @throws ManagerException 如果调用 <code>Driver.getConnection</code> 失败，则抛出该异常。
     */
    public abstract boolean connect(String url, String user, String pass) throws ManagerException;

    /**
     * 使用指定的host、port和用户名密码连接数据库
     * @param host 主机名称
     * @param port 服务端口
     * @param user 用户名称
     * @param pass 用户密码
     * @param db   默认数据库
     * @return 返回 true 表示连接成功， 返回 false 表示连接失败。
     * @throws ManagerException 如果调用 <code>Driver.getConnection</code> 失败，则抛出该异常。
     */
    public abstract boolean connect(String host, int port,
                                    String user, String pass, String db) throws ManagerException;

    /**
     * 预编译传参SQL语句。
     * @param inSqlStr 预编译 SQL 语句
     * @param parameters 参数
     * @return 返回编译完成后的 SQL 语句。
     */
    private String preparedSQL(String inSqlStr, Object... parameters) {
        //1 如果没有参数，说明是不是动态SQL语句
        int paramNum = 0;
        if (null != parameters)  paramNum = parameters.length;
        if (1 > paramNum) return inSqlStr;
        //2 如果有参数，则是动态SQL语句
        StringBuilder result = new StringBuilder();
        String[] fields = inSqlStr.split("\\?");
        for (int i = 0; i < paramNum; i++) {
            result.append(fields[i]).append(StringUtils.parseObject(parameters[i]));
        }
        if (fields.length > parameters.length) {
            result.append(fields[fields.length - 1]);
        }
        return result.toString();
    }

    // 填补参数
    private void fillParameter(PreparedStatement pstm, Object... parameters) throws SQLException {
        if (parameters != null) {
            for(int i = 1, length = parameters.length; i <= length; i++) {
                pstm.setObject(i, parameters[i - 1]);
            }
        }
    }

    /**
     * 批量执行 SQL 预编译语句。
     * @param inSqlStr 预编译 SQL 语句（语句中带有 ? ）。
     * @param limits 每次执行的最大 SQL 语句数量。
     * @param parameters 预编译参数组，参数组中每组参数的个数要不得少于预编译 SQL 语句中的 ? 数量。
     * @return 如果执行成功，则返回执行结果数组，如果返回 null，则表示参数组为空。
     * @throws ManagerException 执行 SQL 时遇到错误则抛出异常，一般是由于 SQL 语法问题所致。
     */
    public synchronized final int[] batchExecute(String inSqlStr, int limits,
                                                 Object[]... parameters) throws ManagerException {
        requireConnection();
        requireConnected();
        if (parameters == null)
            return null;
        List<Integer> result = new LinkedList<>();
        try (PreparedStatement pstm = conn.prepareStatement(inSqlStr)) {
            int length = parameters.length, batchs = 0;
            //int limits = 1000;
            while (length > 0) {
                int rows = length > limits ? limits : length;
                //System.out.println("executeBatch, start = " + batchs + ", end = " + (batchs + rows) );
                for (int i = batchs, size = (batchs + rows); i < size; i++) {
                    fillParameter(pstm, parameters[i]);
                    pstm.addBatch();
                }
                // 批量执行SQL
                for (int x : pstm.executeBatch()) {
                    result.add(x);
                }
                pstm.clearBatch();
                // 更新计数
                batchs += rows;
                length -= rows;
                // 记录最后连接时间
                recordLastConnectionTime();
            }
            return result.stream().mapToInt(e -> e).toArray();//stmt.executeBatch();
        } catch (SQLException e) {
            throw new ManagerException(e.getMessage(), e);
        }
    }

    /**
     * 批量执行 SQL 语句
     * @param limits 每次执行的最大语句数据。
     * @param sqls SQL 语句
     * @return 返回执行结果数组，每一条语句对应数组成员。
     * @throws ManagerException 执行 SQL 时遇到错误则抛出异常，一般是由于 SQL 语法问题所致。
     */
    public synchronized final int[] batchExecute(int limits, String... sqls) throws ManagerException {
        requireConnection();
        requireConnected();
        if (sqls == null)
            return null;
        try (Statement stmt = conn.createStatement()) {
            int length = sqls.length, batchs = 0;
            List<Integer> result = new LinkedList<>();
            while (length > 0) {
                int rows = length > limits ? limits : length;
                //System.out.println("executeBatch, start = " + batchs + ", end = " + (batchs + rows) );
                for (int i = batchs, size = (batchs + rows); i < size; i++) {
                    stmt.addBatch(sqls[i]);
                }
                // 批量执行SQL
                for (int x : stmt.executeBatch()) {
                    result.add(x);
                }
                stmt.clearBatch();
                batchs += rows;
                length -= rows;
                // 记录最后连接时间
                recordLastConnectionTime();
            }
            return result.stream().mapToInt(e -> e).toArray();//stmt.executeBatch();
        } catch (SQLException e) {
            throw new ManagerException(e.getMessage(), e);
        }
    }

    /**
     * 批量执行 SQL 语句
     * @param sqls SQL 语句
     * @return 返回执行结果数组，每一条语句对应数组成员。
     * @throws ManagerException 执行 SQL 时遇到错误则抛出异常，一般是由于 SQL 语法问题所致。
     */
    public synchronized final int[] batchExecute(String... sqls) throws ManagerException {
        return batchExecute(10000, sqls);
    }

    /**
     * 批量插入数据
     * @param inSqlStr SQL语句。必须以 "insert" 开头。
     * @param limits 每批插入数据量限制。
     * @param parameters 待插入的数据。即 SQL 语句中 "values" 后所需参数部分。
     * @return 返回插入的数据量。如果使用了 ignore ，其数值可能不会与 parameters.length  一致。
     * @throws ManagerException 如果出现SQL语法错误，则抛出该异常。
     */
    public synchronized final int batchInsert(String inSqlStr, int limits,
                                              Object[]... parameters) throws ManagerException {
        requireConnection();
        requireConnected();
        int retval = 0;
        try (Statement stmt = conn.createStatement()) {
            if (parameters != null) {
                int length = parameters.length, batchs = 0;
                String execute = inSqlStr.substring(0, inSqlStr.indexOf("values(") + "values".length());
                while (length > 0) {
                    //if (length <= 0) break;
                    int rows = length > limits ? limits : length;
                    String[] values = new String[rows];
                    for (int i = batchs, size = (batchs + rows); i < size; i++) {
                        String[] row = Arrays.stream(parameters[i])
                                .map(StringUtils::parseObject)
                                .toArray(String[]::new);
                        values[i - batchs] = "(" + String.join(",", row) + ")";
                    }
                    String executeSQL = execute + String.join(",", values);
                    //String spilt = executeSQL.length() < 4096 ? executeSQL : executeSQL.substring(0, 4096);
                    //System.out.println("batchInsert: " + spilt +
                    //        "......, start = " + batchs + ", end = " + (batchs + rows) );
                    retval += stmt.executeUpdate(executeSQL);
                    batchs += rows;
                    length -= rows;
                    // 记录最后连接时间
                    recordLastConnectionTime();
                }
            }
            return retval;
        } catch (SQLException e) {
            throw new ManagerException(e.getMessage(), e);
        }
    }

    /**
     * 批量向指定的数据库和数据表插入数据
     * @param database   数据库
     * @param tableName  数据表
     * @param limits     每次插入数据最大限制条数
     * @param ignore     是否忽略重复数据，此项需要数据表指定主键才能有效。
     *                   对于 MySQL 等关系型数据库有效，对于非关系型数据库（无主键约束）无效。
     * @param fieldNames 指定字段，不指定时填 null 即可。
     * @param parameters 待写入的数据组时。指定字段时，每组数据的个数要不得少于指定字段数量，且数据类型要与表字段属性一致。
     *                   未指定字段时，关键约束字段位置不能为空，且数据数量和数据类型要与表字段属性一致。
     * @return 返回插入的数据条数。
     * @throws ManagerException 执行 SQL 时遇到错误则抛出异常，一般是由于 SQL 语法问题所致。
     */
    public synchronized final int batchInsert(String database, String tableName, int limits, boolean ignore,
                                              String[] fieldNames,Object[]... parameters) throws ManagerException {
        //requireConnection();
        //requireConnected();
        //Statement stmt = null;
        if (parameters != null) {
            StringBuilder builder = new StringBuilder();
            builder.append("insert ").append(ignore ? "ignore " : " ").append("into ");
            builder.append(database != null ? database + "." : "").append(tableName);
            if (fieldNames != null) {
                String header = " (`" +  String.join("`, `", fieldNames) + "`)";
                String values = String.join(", ",Arrays.stream(fieldNames).map(e-> "?").toArray(String[]::new));
                builder.append(header).append(" values(").append(values).append(')');
            } else {
                String values = String.join(", ",Arrays.stream(parameters[0]).map(e-> "?").toArray(String[]::new));
                builder.append(" values(").append(values).append(')');
            }
            return batchInsert(builder.toString(), limits, parameters);
        }
        return -1;
    }

    /**
     * 批量插入数据。
     * @param tableName 表名
     * @param limits 每次执行的最大语句数据。
     * @param ignore 是否忽略重复主键
     * @param fieldNames 字段
     * @param parameters 待插入的数据
     * @return 返回插入的数据量。如果使用了 ignore ，其数值可能不会与 parameters.length  一致。
     * @throws ManagerException 执行 SQL 时遇到错误则抛出异常，一般是由于 SQL 语法问题所致。
     */
    public synchronized final int batchInsert(String tableName, int limits, boolean ignore,
                                              String[] fieldNames,Object[]... parameters) throws ManagerException {
        int pos = tableName.indexOf('.');
        String database = pos > 0 ? tableName.substring(0, pos) : conf.getDatabase();
        String name     = pos > 0 ? tableName.substring(pos + 1) : tableName;
        return batchInsert(database, name, limits, ignore, fieldNames, parameters);
    }

    /**
     * 批量合并。
     * @param database    指定数据库
     * @param tableName   指定数据表
     * @param primaryKeys 指定主键。即遇到主键相同的数时，会进行合并。
     * @param parameters  待合并的数据
     * @return 返回合并数据的结果条数。
     * @throws ManagerException 执行 SQL 时遇到错误则抛出异常，一般是由于 SQL 语法问题所致。
     * @throws UnsupportedOperationException 该方法需要重写实现，如果没有实现，则抛出该异常。
     */
    public synchronized int batchMerge(String database, String tableName, String[] primaryKeys,
                                       Map<String, Object>... parameters) throws ManagerException {
        throw new UnsupportedOperationException();
    }

    /**
     * 批量更新
     * @param limits 每次执行的最大语句数据。
     * @param inSqlStr 执行的 SQL 语句。
     * @return 返回执行结果数组，每一条语句对应数组成员。
     * @throws ManagerException 执行 SQL 时遇到错误则抛出异常，一般是由于 SQL 语法问题所致。
     */
    public synchronized final int[] batchUpdate(int limits, String... inSqlStr) throws ManagerException {
        //String lower = inSqlStr != null ? inSqlStr.substring(0, inSqlStr.indexOf(' ')).toLowerCase() : "";
        //if (lower.startsWith("update") || lower.startsWith("insert") || lower.startsWith("delete"))
        //    return batchExecute(limits, inSqlStr);
        //else
        //    throw new IllegalArgumentException("Parameter inSqlStr is invalid." +
        //           " Because it not head within update/insert/delete.");
        return batchExecute(limits, inSqlStr);
    }

    /**
     * 批量执行预编译更新语句，如 UPDATE, INSERT, DELETE 等等。
     * @param inSqlStr 预编译 SQL 语句（语句中带有 ? ）。
     * @param limits 每次执行的最大 SQL 语句数量。
     * @param parameters 预编译参数组，参数组中每组参数的个数要不得少于预编译 SQL 语句中的 ? 数量。
     * @return 如果执行成功，则返回执行结果数组，如果返回 null，则表示参数组为空。
     * @throws ManagerException 执行 SQL 时遇到错误则抛出异常，一般是由于 SQL 语法问题所致。
     */
    public synchronized final int[] batchUpdate(String inSqlStr, int limits,
                                                Object[]... parameters) throws ManagerException {
        String lower = inSqlStr != null ? inSqlStr.substring(0, inSqlStr.indexOf(' ')).toLowerCase() : "";
        if (lower.startsWith("update") || lower.startsWith("insert") || lower.startsWith("delete"))
            return batchExecute(inSqlStr, limits, parameters);
        else
            throw new IllegalArgumentException("Parameter inSqlStr is invalid." +
                    " Because it not head within update/insert/delete.");

    }

    /**
     * 批量执行预编译更新语句，如 UPDATE, INSERT, DELETE 等等。默认每次执行 1000 条 SQL 语句。
     * @param inSqlStr 预编译 SQL 语句（语句中带有 ? ）。
     * @param parameters 预编译参数组，参数组中每组参数的个数要不得少于预编译 SQL 语句中的 ? 数量。
     * @return 如果执行成功，则返回执行结果数组，如果返回 null，则表示参数组为空。
     * @throws ManagerException 执行 SQL 时遇到错误则抛出异常，一般是由于 SQL 语法问题所致。
     */
    public synchronized final int[] batchUpdate(String inSqlStr, Object[]... parameters) throws ManagerException {
        return batchUpdate(inSqlStr, 1000, parameters);
    }

    /**
     * 提交。
     * <p>
     *     如果设置了 autocommit，则不需要执行操作。
     * </p>
     * @throws ManagerException 执行过程中出错，则抛出该异常。
     */
    public synchronized final void commit() throws ManagerException{
        requireConnection();
        requireConnected();
        try {
            if(!conn.getAutoCommit())
                conn.commit();
        } catch (SQLException e) {
            throw new ManagerException(e.getMessage(), e);
        }
    }

    /**
     * 关闭连接。在 GC 阶段，此功能会自动执行。
     */
    @Override
    public synchronized final void close() {
        try {
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            conn = null;
        }
    }

    // 执行
    private synchronized boolean execute0(String inSqlStr, Object... parameters) throws ManagerException {
        requireConnection();
        requireConnected();
        try (PreparedStatement pstm = conn.prepareStatement(inSqlStr)){
            fillParameter(pstm, parameters);
            boolean result = pstm.execute();
            // 记录最后连接时间
            recordLastConnectionTime();
            return result;
        } catch (SQLException e) {
            throw new ManagerException(e.getMessage(), e);
        }
    }

    /**
     * 执行预编译 SQL 语句。可以是任何语句，包括 DDL/DML/DCL 等等。
     * @param inSqlStr 预编译 SQL 语句（语句中带有 ? ）。
     * @param parameters 预编译参数组，参数组中每组参数的个数要不得少于预编译 SQL 语句中的 ? 数量。
     * @return 如果属于 UPDATE/INSERT/DELETE/QUERY 语句，执行成功后，如果结果数量大于 0，返回 true。
     *         其他语句一般返回 false。
     * @throws ManagerException 执行 SQL 时遇到错误则抛出异常，一般是由于 SQL 语法问题所致。
     */
    public synchronized final boolean execute(String inSqlStr, Object... parameters) throws ManagerException {
        return execute0(inSqlStr, parameters);
    }

    /**
     * 执行 SQL 语句。可以是任何语句，包括 DDL/DML/DCL 等等。
     * @param inSqlStr SQL 语句
     * @return 如果属于 UPDATE/INSERT/DELETE/QUERY 语句，执行成功后，如果结果数量大于 0，返回 true。
     *         其他语句一般返回 false。
     * @throws ManagerException 执行 SQL 时遇到错误则抛出异常，一般是由于 SQL 语法问题所致。
     */
    public synchronized final boolean execute(String inSqlStr) throws ManagerException {
        return execute0(inSqlStr);
    }

    /**
     * 检测是否存在指定的数据库。
     * @param database 数据库
     * @return 返回 true 表示存在， 返回 false 表示不存在。
     * @throws ManagerException 执行时遇到错误则抛出异常。
     * @throws UnsupportedOperationException 该方法需要重写实现，如果没有实现，则抛出该异常。
     */
    public synchronized boolean existsDatabase(String database) throws ManagerException {
        throw new UnsupportedOperationException();
    }

    /**
     * 检测是否存在指定的数据表。
     * @param database 数据库
     * @param tableName 数据表
     * @return 返回 true 表示存在， 返回 false 表示不存在。
     * @throws ManagerException 执行时遇到错误则抛出异常。
     * @throws UnsupportedOperationException 该方法需要重写实现，如果没有实现，则抛出该异常。
     */
    public synchronized boolean existsTable(String database, String tableName) throws ManagerException {
        throw new UnsupportedOperationException();
    }

    /**
     * 检测是否存在指定的数据视图。
     * @param database 数据库
     * @param viewName 数据视图
     * @return 返回 true 表示存在， 返回 false 表示不存在。
     * @throws ManagerException 执行时遇到错误则抛出异常。
     * @throws UnsupportedOperationException 该方法需要重写实现，如果没有实现，则抛出该异常。
     */
    public synchronized boolean existsView(String database, String viewName) throws ManagerException {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized ManagerConfig getConf() {
        return conf;
    }

    // 记录最后连接时间
    protected void recordLastConnectionTime() {
        last = System.currentTimeMillis();
    }

    /**
     * 获取最后连接时间
     * @return 返回最后连接时间。
     */
    public long getLastConnectionTime() {
        return last;
    }

    /**
     * 获取客户端信息。
     * @return 返回客户端信息。
     * @throws ManagerException 执行过程中出错则抛出该异常。
     */
    public synchronized Map<String, String> getClientInfo() throws ManagerException {
        requireConnection();
        requireConnected();
        Map<String, String> properties = new HashMap<>();
        try (ResultSet rs = conn.getMetaData().getClientInfoProperties();) {
            while(rs .next()) {
                String name = rs.getString("NAME");
                String value = rs.getString("DEFAULT_VALUE");
                properties.put(name, value);
                // 记录最后连接时间
                recordLastConnectionTime();
            }
            return properties;
        } catch (SQLException e) {
            throw new ManagerException(e.getMessage(), e);
        }
    }

    /**
     * 获取数据库列表
     * @return 返回数据库信息，其类型是 <code>List</code>。
     * @throws ManagerException 执行时遇到错误则抛出异常。
     * @throws UnsupportedOperationException 该方法需要重写实现，如果没有实现，则抛出该异常。
     */
    public synchronized List<? extends Database> getDatabases() throws ManagerException {
        throw new UnsupportedOperationException();
    }

    /**
     * 获取数据库配置信息
     * @return 返回配置信息，其类型是 <code>Map</code>。
     * @throws ManagerException 执行时遇到错误则抛出异常。
     */
    public synchronized Map<String, Object> getDatabaseMeta() throws ManagerException {
        requireConnection();
        requireConnected();
        Map<String, Object> dbMetaData = new HashMap<>();
        DatabaseMetaData dbMeteData;
        try {
            dbMeteData = conn.getMetaData();
            dbMetaData.put("connection.url", dbMeteData.getURL());
            dbMetaData.put("database.name", dbMeteData.getDatabaseProductName());
            dbMetaData.put("database.version.major", dbMeteData.getDatabaseMajorVersion());
            dbMetaData.put("database.version.minor", dbMeteData.getDatabaseMinorVersion());
            dbMetaData.put("sql.kewords", dbMeteData.getSQLKeywords());
            dbMetaData.put("driver.name", dbMeteData.getDriverName());
            dbMetaData.put("driver.version", dbMeteData.getDriverVersion());
            dbMetaData.put("jdbc.version.major", dbMeteData.getJDBCMajorVersion());
            dbMetaData.put("jdbc.version.minor", dbMeteData.getJDBCMinorVersion());
            dbMetaData.put("user.name", dbMeteData.getUserName());
            // 记录最后连接时间
            recordLastConnectionTime();
        } catch (SQLException e) {
            throw new ManagerException(e.getMessage(), e);
        }
        return dbMetaData;
        //throw new UnsupportedOperationException();
    }

    /**
     * 获取所有数据库配置变量
     * @return 返回配置信息
     * @throws ManagerException 执行时遇到错误则抛出异常。
     * @throws UnsupportedOperationException 该方法需要重写实现，如果没有实现，则抛出该异常。
     */
    public synchronized Map<String, String> getVariables() throws ManagerException {
        throw new UnsupportedOperationException();
    }

    /**
     * 获取指定名称的数据库配置变量
     * @param name 配置项名称
     * @return 返回指定名称所对应的值。
     * @throws ManagerException 执行时遇到错误则抛出异常。
     */
    public synchronized String getVariable(String name) throws ManagerException {
        throw new UnsupportedOperationException();
    }

    /**
     * 获取驱动名称
     * @return 返回驱动名称
     * @throws ManagerException 执行时遇到错误则抛出异常。
     */
    public synchronized String getDriverName() throws ManagerException {
        requireConnection();
        requireConnected();
        DatabaseMetaData dbMeteData;
        String driverName;
        try {
            dbMeteData = this.conn.getMetaData();
            driverName = dbMeteData.getDriverName();
            // 记录最后连接时间
            recordLastConnectionTime();
        } catch (SQLException e) {
            throw new ManagerException(e.getMessage(),e);
        }
        return driverName;
    }

    /**
     * 获取所有数据表
     * @return 返回数据表信息，其类型是 <code>List</code>。
     * @throws ManagerException 执行时遇到错误则抛出异常。
     * @throws UnsupportedOperationException 该方法需要重写实现，如果没有实现，则抛出该异常。
     */
    public synchronized List<? extends Table> getTables() throws ManagerException {
        throw new UnsupportedOperationException();
    }

    /**
     * 获取数据库指定库中的所有表
     * @param database 数据库
     * @return 返回数据表信息，其类型是 <code>List</code>。
     * @throws ManagerException 执行时遇到错误则抛出异常。
     * @throws UnsupportedOperationException 该方法需要重写实现，如果没有实现，则抛出该异常。
     */
    public synchronized List<? extends Table> getTables(String database) throws ManagerException {
        throw new UnsupportedOperationException();
    }

    /**
     * 获取指定数据表
     * @param database 数据库
     * @param tableName 数据表
     * @return 返回数据表信息，其类型是 <code>Table</code>。
     * @throws ManagerException 执行时遇到错误则抛出异常。
     * @throws UnsupportedOperationException 该方法需要重写实现，如果没有实现，则抛出该异常。
     */
    public synchronized Table getTable(String database, String tableName) throws ManagerException {
        throw new UnsupportedOperationException();
    }

    /**
     * 获取视图表
     * @return 返回视图列表。
     * @throws ManagerException 执行时遇到错误则抛出异常。
     * @throws UnsupportedOperationException 该方法需要重写实现，如果没有实现，则抛出该异常。
     */
    public synchronized List<? extends View> getViews() throws ManagerException {
        throw new UnsupportedOperationException();
    }

    /**
     * 获取视图表
     * @param database 指定数据库
     * @return 返回视图列表。
     * @throws ManagerException 执行时遇到错误则抛出异常。
     * @throws UnsupportedOperationException 该方法需要重写实现，如果没有实现，则抛出该异常。
     */
    public synchronized List<? extends View> getViews(String database) throws ManagerException {
        throw new UnsupportedOperationException();
    }

    // 整理查询的结果字段
    private synchronized Schema[] getFields(ResultSetMetaData rsmd) throws SQLException {
        int length = rsmd.getColumnCount();
        Column[] retval = new Column[length];
        for (int i = 1; i <= length; i++) {
            int typeId = rsmd.getColumnType(i);
            Column column = new Column(rsmd.getColumnLabel(i)
                    , Type.lookup(typeId)
                    , rsmd.getScale(i)
                    , rsmd.getPrecision(i));
            column.setNullable(rsmd.isNullable(i) != ResultSetMetaData.columnNoNulls);
            //column.setPrimary(rsmd.);
            retval[i - 1] = column; //rsmd.getColumnLabel(i);
        }
        return retval;
    }

    /**
     * 检测数据库是否连接
     * @return 返回 true 表示已连接， 返回 false 表示未连接。
     */
    @Override
    public synchronized boolean isConnected() {
        if (conn == null) return false;
        try {
            return !conn.isClosed();
            //return conn.isValid(3000);
        } catch (SQLException e) {
            // 遇到异常表示数据库连接异常或连接断开。
            return false;
        }
    }

    /**
     * 合并数据。当遇到相同主键时，可以更新原有主键的数据，没有则插入为新数据。（部分数据库支持）
     * @param database    指定数据库
     * @param tableName   指定表
     * @param primaryKeys 指定主键。用于指定 Map 数据中哪些键属于主键。
     * @param values      指定值。Map 类型的数据。
     * @return 返回合并数量。
     * @throws ManagerException 执行过程中出现异常，则抛出该异常。
     * @throws UnsupportedOperationException 该方法需要重写实现，如果没有实现，则抛出该异常。
     */
    public synchronized int merge(String database, String tableName,
                                  String[] primaryKeys, Map<String, Object> values) throws ManagerException {
        throw new UnsupportedOperationException();
    }

    /**
     * 合并数据。当遇到相同主键时，可以更新原有主键的数据，没有则插入为新数据。（部分数据库支持）
     * @param database    指定数据库
     * @param tableName   指定表
     * @param values      指定值。Row 类型的数据，通过 toMap 功能获取字段和数据。
     * @return 返回合并数量。
     * @throws ManagerException 执行过程中出现异常，则抛出该异常。
     * @throws UnsupportedOperationException 该方法需要重写实现，如果没有实现，则抛出该异常。
     */
    public synchronized int merge(String database, String tableName, Row values) throws ManagerException {
        throw new UnsupportedOperationException();
    }

    // 查询
    private synchronized JDBCRecord query0(String inSqlStr, Object... parameters) throws ManagerException {
        requireConnection();
        requireConnected();
        //ResultSet rs;
        //ResultSetMetaData rsmd = null;
        //Schema [] fields = null;
        PreparedStatement pstmt ;
        JDBCRecord retval;
        try {
            pstmt = conn.prepareStatement(inSqlStr);
            fillParameter(pstmt, parameters);
            //ResultSet rs = pstm.executeQuery();
            retval = new JDBCRecord(this, pstmt, null, pstmt.executeQuery());
            /*while (rs.next()) {
                // 记录最后连接时间
                recordLastConnectionTime();
                if (fields == null) {
                    fields = getFields(rsmd = rs.getMetaData());
                }
                DataRow row = new DataRow(fields);
                for (int i = 1, length = rsmd.getColumnCount(); i <= length; i++) {
                    row.setValue(i - 1, rs.getObject(i));
                }
                retval.add(row);
            }*/
            return retval;
        } catch (SQLException e) {
            throw new ManagerException(e.getMessage(), e);
        }
    }

    /**
     * 查询数据
     * @param inSqlStr 查询语句
     * @param parameters 参数
     * @return 返回查询结呆。数据类型为：List，单条数据使用 Row 进行操作。
     * @throws ManagerException 查询过程中出错，则抛出该异常。
     */
    public synchronized final JDBCRecord query(String inSqlStr, Object... parameters) throws ManagerException {
        return query0(inSqlStr, parameters);
    }

    /**
     * 查询数据
     * @param inSqlStr 查询语句
     * @return 返回查询结呆。数据类型为：List，单条数据使用 Row 进行操作。
     * @throws ManagerException 查询过程中出错，则抛出该异常。
     */
    public synchronized final JDBCRecord query(String inSqlStr) throws ManagerException {
        return query0(inSqlStr);
    }

    /**
     * 回滚。
     * <p>
     *     回退在此之前提交的事务。
     * </p>
     * @throws ManagerException 执行过程中出错，则抛出该异常。
     */
    public synchronized final void rollback() throws ManagerException {
        requireConnection();
        requireConnected();
        try {
            if(conn!=null)
                conn.rollback();
        } catch (SQLException e) {
            throw new MySQLException(e.getMessage(), e);
        }
    }

    // 更新
    private synchronized int update0(String inSqlStr, Object... parameters) throws ManagerException {
        requireConnection();
        requireConnected();
        try (PreparedStatement pstm = conn.prepareStatement(inSqlStr)) {
            fillParameter(pstm, parameters);
            int result = pstm.executeUpdate();
            // 记录最后连接时间
            recordLastConnectionTime();
            return result;
        } catch (SQLException e) {
            throw new ManagerException(e.getMessage() + " >> " + inSqlStr, e);
        }
    }

    /**
     * 更新数据
     * @param inSqlStr 更新语句。如： DELETE、INSERT、REPLACE、UPDATE 等等。
     * @param parameters 参数
     * @return 返回更新语句影响的结果数量。
     * @throws ManagerException 执行过程中出错，则抛出该异常。
     */
    public synchronized final int update(String inSqlStr, Object... parameters) throws ManagerException {
        return update0(inSqlStr, parameters);
    }

    /**
     * 更新数据
     * @param inSqlStr 更新语句。如： DELETE、INSERT、REPLACE、UPDATE 等等。
     * @return 返回更新语句影响的结果数量。
     * @throws ManagerException 执行过程中出错，则抛出该异常。
     */
    public synchronized final int update(String inSqlStr) throws ManagerException {
        return update0(inSqlStr);
    }

}
