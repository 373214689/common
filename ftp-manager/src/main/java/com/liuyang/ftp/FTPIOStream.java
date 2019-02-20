package com.liuyang.ftp;

import java.io.InputStream;
import java.io.OutputStream;

@FunctionalInterface
public interface FTPIOStream {
    long accept(InputStream in, OutputStream out);
}
