package com.liuyang.common;

import java.net.URI;

/**
 * Manager Config Interface
 * <p/>
 * 管理器配置接口。
 * 只提取读取操作功能接口, 写入操作功能需在实体类中作出相应的实现。
 *
 * @author liuyang
 * @version 1.0.0
 *
 */
public interface ManagerConfig {

    /**
     * 创建连接。
     *
     * @return 返回一个可用的连接
     */
    ManagerClient getConnection() throws ManagerException;

    /**
     * 获取传输编码方式
     * @return 返回传输编码。如 UTF8, GBK 等等。
     */
    String getEncoding();

    /**
     * 获取主机。
     * URI格式 => [schema]://[user:pass]@[host:port]/[path]?[query|name=value&n1=v1]
     * @return 返回主机名
     */
    String getHost();

    /**
     * 获取端口号。
     * URI格式 => [schema]://[user:pass]@[host:port]/[path]?[query|name=value&n1=v1]
     * @return 返回主机端口
     */
    int getPort();

    /**
     * 获取用户名。
     * URI格式 => [schema]://[user:pass]@[host:port]/[path]?[query|name=value&n1=v1]
     * @return 返回用户名
     */
    String getUser();

    /**
     * 获取密码。
     * URI格式 => [schema]://[user:pass]@[host:port]/[path]?[query|name=value&n1=v1]
     * @return 返回用户密码
     */
    String getPass();

    /**
     * 获取路径。
     * URI格式 => [schema]://[user:pass]@[host:port]/[path]?[query|name=value&n1=v1]
     * @return 返回路径
     */
    String getPath();

    /**
     * 获取查询参数。
     * URI格式 => [schema]://[user:pass]@[host:port]/[path]?[query|name=value&n1=v1]
     * @param name 指定名称
     * @return 返回指定名称传回的参数。
     */
    String getParameter(String name);

    /**
     * 获取查询语句。
     * URI格式 => [schema]://[user:pass]@[host:port]/[path]?[query|name=value&n1=v1]
     * @return 返回查询语句
     */
    String getQuery();

    /**
     * 获取模式。
     * 诸如：ftp, http, sftp, ssh 等等。
     * URI格式 => [schema]://[user:pass]@[host:port]/[path]?[query|name=value&n1=v1]
     * @return 返回配置协议模式。
     */
    String getSchema();

    /**
     * 是否使用 SSL （加密套接字协议层）传输数据
     * @return 返回 true 表示使用 SSL，返回 false 表示不使用。
     */
    boolean isUseSSL();

    /**
     * 转换为URI (Uniform Resource Identifier, 统一资源标识符)。
     * @return 返回 URI 数据
     * @throws Exception 如果出现 URI 的语法错误，则抛出该异常。
     */
    URI toURI() throws Exception;

    /**
     * 解析URI (Uniform Resource Identifier, 统一资源标识符)。
     * @param uri 指定超链接
     * @throws Exception 无法解析 URI 时，抛出异常。
     */
    void parseURI(URI uri) throws Exception;

}
