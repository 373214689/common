package com.liuyang.hadoop;

import com.liuyang.common.AbstractManagerConfig;

public class HDFSConfig extends AbstractManagerConfig {

    private  HDFSManager manager;

    public HDFSConfig(String host, int port, String user, String pass) {
        this.schema = "hdfs";
        this.host = host;
        this.port = port;
        this.user = user;
        this.pass = pass;
    }

    public HDFSConfig(String host, int port, String user, String pass, String path) {
        this(host, port, user, pass);
        this.path = path;
    }

    public synchronized HDFSManager getConnection() throws HDFSException {
        if (manager == null)
            manager = new HDFSManager();
        manager.connect(this);
        return manager;
    }

    @Deprecated
    public String getParameter(String name) {
        return null;
    }

    @Deprecated
    public String getQuery() {
        return null;
    }

    @Override
    public String toString() {
        return String.format("%s://%s:%d", schema, host, port);
    }


}
