package com.liuyang.common;

import com.sun.istack.internal.NotNull;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.function.Function;

public abstract class AbstractManagerConfig implements ManagerConfig {

    protected String schema;
    /** 配置名称 */
    protected String name = "";
    /** 配置描述 */
    protected String desc = "";
    protected String host;
    protected int    port = -1;
    protected String user;
    protected String pass;
    protected String path = "";
    protected String charset = "UTF8";
    protected boolean useSSL = false;

    protected AbstractManagerConfig() {

    }

    @Override
    protected void finalize() {
        clear();
    }

    // 清理数据
    protected void clear() {
        schema = null;
        name = null;
        desc = null;
        host = null;
        port = 0;
        user = null;
        pass = null;
        charset = null;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null)
            return false;
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

    public String getPath() {
        return path;
    }

    public String getSchema() {
        return schema;
    }

    public boolean isUseSSL() {
        return useSSL;
    }

    public void setHost(@NotNull String host) {
        this.host = host;
    }

    public void setName(@NotNull String name) {
        this.name = name;
    }

    public void setName(@NotNull Function<String, String> action) {
        this.name = action.apply(name);
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setUser(@NotNull String user) {
        this.user = user;
    }

    public void setPass(@NotNull String pass) {
        this.pass = pass;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void setDescritpion(String description) {
        this.desc = description;
    }

    public void setUseSSL(boolean flag) {
        this.useSSL = true;
    }

    @Override
    public String toString() {
        return String.format("%s://%s:%s@%s:%d%s"
                , schema
                , user
                , pass
                , host
                , port
                , path);
    }

    public URI toURI() throws URISyntaxException, UnsupportedEncodingException {
        return URI.create(String.format("%s://%s:%s@%s:%d%s"
                , schema
                , user
                , URLEncoder.encode(pass, "UTF-8")
                , host
                , port
                , path));
    }

    public void parseURI(@NotNull URI uri) throws UnsupportedEncodingException {
        String shcema = uri.getScheme();
        String host = uri.getHost();
        int    port = uri.getPort();
        String path = uri.getPath();
        String userInfo = uri.getUserInfo();
        String user = null;
        String pass = null;
        if (userInfo != null) {
            int index = userInfo.indexOf(':');
            user = userInfo.substring(0, index++);
            pass = URLDecoder.decode(userInfo.substring(index), "UTF-8");
        }
        //Pattern p = Pattern.compile("(ftp://)([w+]");
        if (!this.schema.equals(shcema))
            throw new IllegalArgumentException("schema is not " + schema);
        if (host == null)
            throw new IllegalArgumentException("can not parse host from uri");
        if (port == -1)
            port = 21;
        this.host = host;
        this.port = port;
        this.user = user;
        this.pass = pass;
        this.path = path;
    }
}
