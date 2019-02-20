package com.liuyang.jdbc;

/**
 * 数据库
 */
public interface Database {
    //private String name;
    //List<Ta>

    String getName();

    String getOwner();

   Table[] getTables();

}
