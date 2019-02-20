package com.liuyang.ftp.client;

public interface FTPResponse {

    public abstract int getCode();
    public abstract String getText();
    public abstract boolean getStatus();
}
