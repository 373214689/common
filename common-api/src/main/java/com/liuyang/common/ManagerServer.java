package com.liuyang.common;

public interface ManagerServer {

    void isRunning();

    void listen(int port);

    void stop();

}
