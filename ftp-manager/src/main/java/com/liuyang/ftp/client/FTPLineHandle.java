package com.liuyang.ftp.client;

@FunctionalInterface
public interface FTPLineHandle {
    void apply(long id, String line);
}
