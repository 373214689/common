package com.liuyang.common;

/**
 * Manager 异常处理接口
 *
 * @author liuyang
 * @version 1.0.0
 */
public class ManagerException extends RuntimeException {

    /** serialVersionUID */
    private static final long serialVersionUID = 1502047624813219891L;

    public ManagerException() {
        super();
    }

    public ManagerException(String message) {
        super(message);
    }

    public ManagerException(String message, Throwable cause) {
        super(message, cause);
    }



}
