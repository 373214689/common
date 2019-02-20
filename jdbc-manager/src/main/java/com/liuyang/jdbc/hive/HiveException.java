package com.liuyang.jdbc.hive;

import com.liuyang.common.ManagerException;

public class HiveException  extends ManagerException {

    public HiveException() {
        super();
    }

    public HiveException(String s) {
        super(s);
    }

    public HiveException(String s, Throwable t) {
        super(s, t);
    }
}