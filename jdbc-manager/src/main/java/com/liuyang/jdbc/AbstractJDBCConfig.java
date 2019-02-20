package com.liuyang.jdbc;

import com.liuyang.common.AbstractManagerConfig;

public abstract class AbstractJDBCConfig extends AbstractManagerConfig{

    /** 默认数据库 */
    protected String database;

    protected AbstractJDBCConfig(String schema, String host, int port, String user, String pass) {
        this.schema = schema;
        this.host   = host;
        this.port   = port;
        this.user   = user;
        this.pass   = pass;
    }

    protected AbstractJDBCConfig(String schema, String host, int port, String user, String pass, String path) {
        this(schema, host, port, user, pass);
        this.path = path;
    }

    protected AbstractJDBCConfig(String schema) {
        this.schema = schema;
    }

    public String getDatabase() {
        return database;
    }

    public String getDriverName() {
        return "";
    }

    @Override
    public String getPath() {
        return database;
    }

    @Deprecated
    public String getQuery() {
        return null;
    }

    @Deprecated
    public String getParameter(String name) {
        return null;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

}
