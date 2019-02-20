package com.liuyang.jdbc;

import com.liuyang.ds.DataRecord;
import com.liuyang.ds.Row;
import com.liuyang.ds.Schema;
import com.liuyang.ds.Type;
import com.liuyang.ds.sets.DataRow;
import com.sun.istack.internal.NotNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * JDBC Record
 *
 * @author liuyang
 * @version 1.0.1
 * @see com.liuyang.ds.DataRecord
 */
public class JDBCRecord implements DataRecord<Row> {

    private ResultSet         result;
    private AbstractManager   manager;
    //private PreparedStatement pstmt;
    //private Statement         stmt;
    private ResultSetMetaData rsmd;
    private Schema[]          fields;

    /**
     * 创建 JDBC 数据记录
     * @param manager 指定 JDBC 管理接口
     * @param pstmt 可选，指定预查询接口
     * @param pstmt 可选，指定查询接口
     * @param result 指定数据集合
     * @throws IllegalArgumentException 当无法解析 ResultSet 元数据时抛出该异常。
     */
    public JDBCRecord(@NotNull AbstractManager manager, PreparedStatement pstmt, Statement stmt,
                      @NotNull ResultSet result) {
        this.manager = manager;
        this.result  = result;
        try {
            this.rsmd   = result.getMetaData();
            this.fields = getFields(rsmd);
        } catch (SQLException e) {
            throw new IllegalArgumentException("Can not read MetaData from ResultSet.", e);
        }
    }

    @Override
    public void close() {
        try {
            if (result != null)
                result.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            result = null;
        }
    }

    @Override
    public Schema[] header() {
        return fields;
    }

    // 整理查询的结果字段
    private Schema[] getFields(ResultSetMetaData rsmd) throws SQLException {
        int length = rsmd.getColumnCount();
        Column[] retval = new Column[length];
        for (int i = 1; i <= length; i++) {
            int typeId = rsmd.getColumnType(i);
            Column column = new Column(rsmd.getColumnLabel(i)
                    , Type.lookup(typeId)
                    , rsmd.getScale(i)
                    , rsmd.getPrecision(i));
            column.setNullable(rsmd.isNullable(i) != ResultSetMetaData.columnNoNulls);
            //column.setPrimary(rsmd.);
            retval[i - 1] = column; //rsmd.getColumnLabel(i);
        }
        return retval;
    }

    /**
     * 以数据流的形式输出
     * @return 返回数据流
     */
    public Stream<Row> stream(){
        Iterator<Row> iter = new Itr();
        // 生成流
        Stream<Row> stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                iter, Spliterator.ORDERED | Spliterator.NONNULL), false);
        stream = stream.onClose(this::close);
        return stream;
    }

    public List<Row> toList() {
        return stream().collect(Collectors.toList());
    }

    // 构造迭代器
    private final class Itr implements Iterator<Row> {
        DataRow           row;
        boolean           ready;
        ResultSetMetaData rsmd;

        Itr() {

        }

        // 处理异常
        private void handleException() {
            try {
                if (result != null)
                    result.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                row = null;
                rsmd = null;
            }
        }

        public boolean hasNext(){
            if (result == null)
                return false;
            if (ready)
                return true;
            try {
                return (ready = result.next());
            } catch (SQLException e) {
                //e.printStackTrace();
                handleException();
                throw new UncheckedIOException(new IOException(e));
            }
        }

        public Row next() {
            if (hasNext()) {
                try {
                    if (row == null) {
                        rsmd = result.getMetaData();
                        row = new DataRow(getFields(rsmd));
                    }
                    for (int i = 1, length = rsmd.getColumnCount(); i <= length; i++) {
                        row.setValue(i - 1, result.getObject(i));
                    }
                    // 记录最后连接时间
                    manager.recordLastConnectionTime();
                    return row;
                } catch (SQLException e) {
                    handleException();
                    throw new NoSuchElementException();
                } finally {
                    ready = false;
                }

            } else {
                handleException();
                throw new NoSuchElementException();
            }

        }
    }
}
