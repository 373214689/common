package com.liuyang.jdbc;

import com.liuyang.ds.Row;
import com.liuyang.ds.Schema;

import java.util.Map;

/**
 * 数据表
 */
public interface Table {

    Database getDatabase();

    String getFullName();

    String getName();

    String getLocation();

    Schema[] getFields();

    Map<String, Object> toMap();

    Row toRow();

}
