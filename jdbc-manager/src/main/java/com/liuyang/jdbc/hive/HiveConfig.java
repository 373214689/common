package com.liuyang.jdbc.hive;

import com.liuyang.jdbc.AbstractJDBCConfig;
import com.sun.istack.internal.NotNull;

import java.net.URI;
import java.net.URISyntaxException;

public class HiveConfig extends AbstractJDBCConfig {
    private final static String HIVE_URI = "%s://%s:%d/%s";

    private HiveManager manager;
    private HiveVersion version;

    public HiveConfig() {
        super("jdbc:hive2");
        super.host = "localhost";
        super.port = 10000;
        super.database = "default";
        this.version = HiveVersion.VERSION_2;
    }

    @Override
    protected void finalize() {
        super.clear();
        if (manager != null) {
            manager.close();
        }
    }

    public HiveConfig(String host, int port, String user, String pass) {
        super("jdbc:hive2", host, port, user, pass);
        super.database = "default";
        this.version = HiveVersion.VERSION_2;
    }

    @Override
    public synchronized HiveManager getConnection() throws HiveException {
        if (manager == null)
            manager = new HiveManager();
        manager.connect(this);
        return manager;
    }

    @Override
    public String getDriverName() {
        return version.getDriver();
    }


    public String toString() {
        return String.format(HIVE_URI, schema, host, port, database);
    }

    public URI toURI() throws URISyntaxException {
        String str = String.format(HIVE_URI, schema, host, port, database);
        return new URI(str);
    }

    public void setVersion(@NotNull HiveVersion version) {
        this.version = version;
        super.schema = version.getSchema();
    }
}
