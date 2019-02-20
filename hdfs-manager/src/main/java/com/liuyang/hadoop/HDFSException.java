package com.liuyang.hadoop;

import com.liuyang.common.ManagerException;

public class HDFSException extends ManagerException {
    public HDFSException(String message) {
        super(message);
    }

    public HDFSException(String message, Throwable cause) {
        super(message, cause);
    }
}
