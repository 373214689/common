package com.liuyang.ds.functions;

import com.liuyang.ds.Row;

@FunctionalInterface
public interface RowFunction {
    Row apply(Row row);
}
