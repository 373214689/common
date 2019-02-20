package com.liuyang.jdbc.mysql;

import com.liuyang.jdbc.AbstractJDBCConfig;

import java.net.URI;
import java.net.URISyntaxException;

public class MySQLConfig extends AbstractJDBCConfig {
    private final static String MYSQL_URI = "%s://%s:%d/%s?useUnicode=true&characterEncoding=UTF-8"
            + "&autoReconnect=true&cachePrepStmts=true&rewriteBatchedStatements=true";

    public MySQLConfig() {
        super("jdbc:mysql");
        host = "localhost";
        port = 3306;
        database = "mysql";
    }

    public MySQLConfig(String host, int port, String user, String pass) {
        super("jdbc:mysql", host, port, user, pass);
        this.database = "mysql";
    }

    public MySQLConfig(String host, int port, String user, String pass, String database) {
        super("jdbc:mysql", host, port, user, pass);
        this.database = database;
    }

    private MySQLManager manager;

    @Override
    protected void finalize() {
        super.clear();
        if (manager != null) {
            manager.close();
        }
    }

    @Override
    public synchronized MySQLManager getConnection() throws MySQLException {
        if (manager == null)
            manager = new MySQLManager();
        manager.connect(this);
        return manager;
    }

    @Override
    public String getDriverName() {
        return "com.mysql.jdbc.Driver";
    }

    public String toString() {
        return String.format(MYSQL_URI, schema, host, port, database);
    }

    public URI toURI() throws URISyntaxException {
        String str = String.format(MYSQL_URI, schema, host, port, database);
        return new URI(str);
    }

}
