package com.liuyang.jdbc.hive;

import com.liuyang.ds.Row;
import com.liuyang.jdbc.AbstractManager;
import com.liuyang.jdbc.mysql.MySQLConfig;
import com.liuyang.jdbc.mysql.MySQLException;
import com.liuyang.tools.StringUtils;
import com.sun.istack.internal.NotNull;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * Hive Connection Manager
 *
 * @author liuyang
 * @version 1.0.0
 */
public class HiveManager extends AbstractManager {
    private final static Map<String, String> STORE_TYPE_MAP = new HashMap<>();

    static {
        STORE_TYPE_MAP.put("org.apache.hadoop.hive.ql.io.orc.OrcSerde", "ORC");
    }

    public HiveManager() { }

    public HiveManager(@NotNull HiveConfig config) {
        super.conf = config;
    }

    /**
     * 连接
     * @param conf 指定 HiveConfig
     * @return 返回 true 表示连接成功，返回 false 表示连接失败。
     * @throws HiveException 执行过程中出错则抛出该异常。
     */
    public synchronized boolean connect(HiveConfig conf) throws HiveException {
        return super.connect(conf);
    }

    @Override
    public synchronized boolean connect(String hostname, int port, String defaultDatabase,
                                        String username, String password) throws HiveException {
        HiveConfig conf = new HiveConfig(hostname, port, username, password);
        conf.setDatabase(defaultDatabase);
        return connect(conf);
    }

    @Override
    public synchronized boolean connect(String url, String user, String pass) throws HiveException {
        MySQLConfig conf = new MySQLConfig();
        try {
            conf.parseURI(new URI(url));
            conf.setUser(user);
            conf.setPass(pass);
            return connect(conf);
        } catch (URISyntaxException e) {
            throw new HiveException("Syntax Error: Can not parse url(" + url + "), " + e.getMessage());
        } catch (UnsupportedEncodingException e) {
            throw new HiveException("Encoding Error: Can not parse url(" + url + "), " + e.getMessage());
        }
    }

    @Override
    public synchronized boolean existsDatabase(String database) throws HiveException {
        if (StringUtils.isEmpty(database))
            return false;
        requireConnection();
        requireConnected();
        // 获取数据库
        try (ResultSet rs = conn.getMetaData().getCatalogs()) {
            // 记录最后连接时间
            recordLastConnectionTime();
            while (rs.next()) {
                if (database.equals(rs.getString("TABLE_CAT")))
                    return true;
            }
        } catch (SQLException e) {
            throw new HiveException(e.getMessage(), e);
        }
        return false;
    }

    @Override
    public synchronized boolean existsTable(String database, String tableName) throws HiveException {
        if (StringUtils.isEmpty(tableName))
            return false;
        requireConnection();
        requireConnected();
        // 获取表
        try (ResultSet rs = conn.getMetaData().getTables(database, null, tableName, new String[]{"TABLE"})){
            // 记录最后连接时间
            recordLastConnectionTime();
            return rs.next();
        } catch (SQLException e) {
            throw new HiveException(e.getMessage(), e);
        }
    }

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
    public synchronized List<HiveDataBase> getDatabases() throws HiveException {
        requireConnection();
        requireConnected();
        List<HiveDataBase> list = new LinkedList<>();
        try (ResultSet rs = conn.getMetaData().getCatalogs()) {
            while(rs.next()) {
                // 记录最后连接时间
                recordLastConnectionTime();
                list.add(new HiveDataBase(rs.getString("TABLE_CAT"), conf.getUser()));
            }
        } catch (SQLException e) {
            throw new HiveException(e.getMessage(), e);
        }
        return list;
    }

    public HiveTable getTable(String database, String tableName) throws HiveException {
        List<Row> result = super.query("DESCRIBE FORMATTED " + database + "." + tableName).toList();
        //System.out.println("query succful");
        HiveTable retval = null;
        if (result.size () > 0) {
            retval = new HiveTable(new HiveDataBase(database, conf.getUser()), tableName);
            // 表结构起始读取行为第3行(index = 2)
            int length = result.size(),
                    columnsStartPos = 2,
                    columnsEndPos = 0,
                    partitionStartPos = 0,
                    partitionEndPos = 0,
                    tableInfoStartPos = 0,
                    tableInfoEndPos = 0,
                    storageInfoStartPos = 0,
                    storageInfoEndPos = 0;
            for(int i = columnsStartPos; i < length; i++) {
                Row row = result.get(i);
                if (columnsStartPos > 0 && row.getString("col_name").isEmpty()) {
                    columnsEndPos = i;
                    break;
                }
            }
            //Schema columns = Schema.createStruct(tableName);
            for(Row row : result.subList(columnsStartPos, columnsEndPos)) {
                retval.addField(row.getString("col_name"), row.getString("data_type"));
            }
            // 表分区在查询到"# Partition Information”字段后，后移3行开始读取
            for(int i = 0; i < length; i++) {
                Row row = result.get(i);
                //System.out.println( i + " >> " + row);
                if (partitionStartPos == 0 && row.getString("col_name").trim().equals("# Partition Information")) {
                    partitionStartPos = i + 3;
                }
                if (partitionStartPos > 0 && partitionStartPos < i && row.getString("col_name").isEmpty()) {
                    partitionEndPos = i;
                    break;
                }
            }

            if (partitionEndPos == 0) partitionEndPos = result.size() - 1;
            for(Row row : result.subList(partitionStartPos, partitionEndPos)) {
                retval.addPartition(row.getString("col_name"), row.getString("data_type"));
            }
            // 表信息在查询到"# Detailed Table Information"字段后，后移2行开始读取
            for(int i = 0; i < length; i++) {
                Row row = result.get(i);
                if (tableInfoStartPos == 0 && row.getString("col_name").trim().equals("# Detailed Table Information")) {
                    tableInfoStartPos = i + 2;
                }
                // 如果发现下行为空
                if (tableInfoStartPos > 0) {
                    if (row.getString("col_name").isEmpty() && !row.getString("data_type").isEmpty()) {
                        row.setValue("col_name", result.get(i - 1).getString("col_name"));
                    }
                }
                if (tableInfoStartPos > 0 && tableInfoStartPos < i && row.getString("col_name").isEmpty()) {
                    tableInfoEndPos = i;
                    break;
                }
            }
            if (tableInfoEndPos == 0) tableInfoEndPos = result.size() - 1;
            for(Row row : result.subList(tableInfoStartPos, tableInfoEndPos)) {
                String col_name = row.getString("col_name").trim();
                String data_type = row.getString("data_type").trim();
                //String comment = row.getString("comment").trim();
                switch(col_name) {
                    case "Database:" : break;
                    case "Owner:" : retval.setOwner(data_type); break;
                    case "CreateTime:" : break;
                    case "LastAccessTime:" : break;
                    case "Retention:" : break;
                    case "Location:" : retval.setLocation(data_type); break;
                    case "Table Type:" : break;
                    case "Table Parameters:" :
                        switch (data_type) {
                            case "COLUMN_STATS_ACCURATE" : break;
                            case "numFiles" : break;
                            case "numPartitions" : break;
                            case "numRows" : break;
                            case "rawDataSize" : break;
                            case "totalSize" : break;
                            case "transient_lastDdlTime" : break;
                        }
                        break;

                }
            }
            // 储存属性在查询到"# Storage Information"字段后，后一行开始读取
            for(int i = 0; i < length; i++) {
                Row row = result.get(i);
                if (storageInfoStartPos == 0 && row.getString("col_name").trim().equals("# Storage Information")) {
                    storageInfoStartPos = i + 1;
                }
                // 如果发现下行为空
                if (storageInfoStartPos > 0) {
                    if (row.getString("col_name").isEmpty() && !row.getString("data_type").isEmpty()) {
                        row.setValue("col_name", result.get(i - 1).getString("col_name"));
                    }
                }
                if (storageInfoStartPos > 0 &&  storageInfoStartPos < i && row.getString("col_name").isEmpty()) {
                    storageInfoEndPos = i;
                    break;
                }
            }
            if (storageInfoEndPos == 0) storageInfoEndPos = result.size() - 1;
            for(Row row : result.subList(storageInfoStartPos, storageInfoEndPos)) {
                String col_name = row.getString("col_name").trim();
                String data_type = row.getString("data_type").trim();
                String comment = row.getString("comment").trim();
                switch(col_name) {
                    case "SerDe Library:" : {
                        String storeType = STORE_TYPE_MAP.get(data_type);
                        if (storeType == null) {
                            storeType = "TEXTFILE";
                        }
                        retval.setStoreType(storeType);
                        break;
                    }
                    case "InputFormat:" : retval.setIntputFormat(data_type); break;
                    case "OutputFormat:" : retval.setOutputFormat(data_type); break;
                    case "Num Buckets:" : break;
                    case "Bucket Columns:" : break;
                    case "Sort Columns:" : break;
                    case "Storage Desc Params:" :
                        switch (data_type) {
                            case "field.delim" : retval.setDelimiter(comment); break;
                            case "serialization.format" : break;
                        }
                        break;
                }
            }
        }
        return retval;
    }
}
