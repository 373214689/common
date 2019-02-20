package com.liuyang.ftp;

@FunctionalInterface
public interface FTPFileFilter {
    boolean filter(FTPFile file);
}
