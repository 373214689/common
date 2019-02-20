package com.liuyang.ftp;

import com.liuyang.common.AbstractManagerConfig;
import com.liuyang.common.ManagerConfig;
import com.liuyang.ftp.client.FTPClient;
import com.sun.istack.internal.NotNull;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;

public class FTPConfig extends AbstractManagerConfig {

    public static FTPConfig parse(String url) {
        try {
            FTPConfig config = new FTPConfig();
            config.parseURI(new URI(url));
            return config;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return null;
    }

    /** 传输模式。主动: true, 被动: false */
    private boolean transMode = false;
    /** 主动传输模式服务初始端口 */
    private int    activePort = 38101;
    /** 本地路径 */
    private String local = "";

    public FTPConfig() {
        this.schema = "ftp";
        this.host = "localhost";
        this.port = 21;
    }

    public FTPConfig(String host, int port, String user, String pass) {
        this.schema = "ftp";
        this.host = host;
        this.port = port;
        this.user = user;
        this.pass = pass;
    }

    public FTPConfig(String host, int port, String user, String pass, String path) {
        this(host, port, user, pass);
        this.path = path;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null)
            return false;
        if (o instanceof FTPConfig) {
            FTPConfig other = (FTPConfig) o;
            //System.out.println("FTPConfig.equals: " + this + " <> " + other);
            return port == other.port
                    && host.equals(other.host)
                    && user.equals(other.user)
                    && pass.equals(other.pass)
                    && path.equals(other.path)
                    && name.equals(other.name);
        }
        if (o instanceof ManagerConfig) {
            ManagerConfig other = (ManagerConfig) o;
            return port == other.getPort()
                    && host.equals(other.getHost())
                    && user.equals(other.getUser())
                    && pass.equals(other.getPass())
                    && path.equals(other.getPath());
        }
        return false;
    }

    /**
     * 开启被动传输模式
     * 与主动传输模式互斥
     *
     */
    public void enablePassiveMode() {
        transMode = false;
    }

    /**
     * 开启主动传输模式
     * 与被动传输模式互斥
     */
    public void enableActiveMode() {
        transMode = false;
    }

    public FTPClient getConnection() {
        return new FTPClient(this);
    }

    public String getSchema() {
        return schema;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return desc;
    }

    public String getEncoding() {
        return charset;
    }


    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUser() {
        return user;
    }

    public String getPass() {
        return pass;
    }

    public String getCharset() {
        return charset;
    }

    public int getActivePort() {
        return activePort;
    }

    @Override
    public String getPath() {
        return path;
    }

    public String getLocalPath() {
        return local;
    }

    @Override
    @Deprecated
    public String getQuery() {
        return null;
    }

    @Override
    @Deprecated
    public String getParameter(String name) {
        return null;
    }

    @Override
    public int hashCode() {
        long hash = host.hashCode() * 31
                + port * 31
                + user.hashCode() * 31
                + pass.hashCode() * 31
                + path.hashCode() * 31
                + name.hashCode() * 31;
        return (int) hash;
    }

    public boolean isPassiveMode() {
        return !transMode;
    }

    public boolean isActiveMode() {
        return transMode;
    }

    public boolean isUseSSL() {
        return false;
    }

    /**
     * 设置配置名称
     * @param name 指定名称
     */
    public void setName(String name) {
        this.name = name;
    }

    public void setDescription(String description) {
        this.desc = description;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setPass(String pass) {
        this.pass = pass;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    /**
     * 设置服务器初始路径
     * @param remotePath 远程服务器上的路径
     */
    public void setPath(String remotePath) {
        this.path = remotePath;
    }

    public void setLocalPath(String localPath) {
        this.local = localPath;
    }

    public void setActivePort(int activePort) {
        this.activePort = activePort;
    }




}


