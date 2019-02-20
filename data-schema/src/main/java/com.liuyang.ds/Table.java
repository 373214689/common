package com.liuyang.ds;

/**
 * 表接口
 *
 */
public interface Table {

    String getDatabase();

    String getName();

    String getLocation();

    String getOwner();

    Schema[] getFields();

}
