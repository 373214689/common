package com.liuyang.ftp;

import com.liuyang.common.ManagerException;

public class FTPClientException extends ManagerException {
    private static final long serialVersionUID = -969900010543273L;

    public FTPClientException(String message) {
        super(message);
    }

    public FTPClientException(String message, Throwable cause) {
        super(message, cause);
    }



}
