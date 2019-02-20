package com.liuyang.common;

/**
 * Manager Client Interface
 * <p/>
 * 管理器客户端接口。
 *
 * @author liuyang
 * @version 1.0.0
 *
 */
public interface ManagerClient {

    /**
     * 获取配置信息。
     *
     * @return 返回配置信息
     */
    ManagerConfig getConf();

    /**
     * 连接。
     * 使用配置连接到服务器。
     *
     * @return 返回 true 表示连接成功， 返回 false 表示连接失败。
     * @throws ManagerException - 如果连接中出现网络或用户数据异常，则抛出该异常。
     */
    boolean connect() throws ManagerException;

    /**
     * 获取最后连接时间。
     * <p>
     *     需要在实际实现中记录相应的时间戳。
     * </p>
     * @return 返回时间戳。
     */
    long getLastConnectionTime();

    /**
     * 判断是否已连接成功。
     *
     * @return 返回 true 表示已连接，返回 false 表示未连接。
     */
    boolean isConnected();

    /**
     * 关闭连接。
     */
    void close();


}
