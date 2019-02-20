package com.liuyang.jdbc.mysql;

import com.liuyang.common.ManagerException;

public class MySQLException extends ManagerException {


    public MySQLException(String s) {
        super(s);
    }

    public MySQLException(String s, Throwable t) {
        super(s, t);
    }
}
