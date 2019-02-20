package com.liuyang.hadoop.data;


import com.liuyang.ds.DataRecord;
import com.liuyang.ds.Row;
import com.liuyang.ds.Schema;
import com.liuyang.ds.attr.Column;
import com.liuyang.ds.sets.DataRow;
import com.liuyang.util.LinkedList;
import com.sun.istack.NotNull;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;

import org.apache.hadoop.io.*;
import org.apache.orc.TypeDescription;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * ORC File Reader
 *
 * @author liuyang
 * @version 1.0.1
 * @see com.liuyang.ds.DataRecord
 */
public final class ORCReader implements DataRecord<Row>, Closeable {
    @SuppressWarnings({"unused"})
    private static Object parse(Object o) {
        if (o == null)
            return null;
        if (o instanceof FloatWritable) {
            return ((FloatWritable) o).get();
        }
        if (o instanceof DoubleWritable) {
            return ((DoubleWritable) o).get();
        }
        if (o instanceof IntWritable) {
            return ((IntWritable) o).get();
        }
        if (o instanceof LongWritable) {
            return ((LongWritable) o).get();
        }
        if (o instanceof ShortWritable) {
            return ((ShortWritable) o).get();
        }
        if (o instanceof BooleanWritable) {
            return ((BooleanWritable) o).get();
        }
        if (o instanceof NullWritable) {
            return null;
        }
        if (o instanceof BytesWritable) {
            return ((BytesWritable) o).getBytes();
        }
        if (o instanceof ByteWritable) {
            return ((ByteWritable) o).get();
        }
        if (o instanceof Text) {
            return o.toString();
        }

        return o;

    }

    /**
     * 读取 ORC 文件
     * @param fs 指定文件系统
     * @param path 指定路径
     * @return 返回流。
     * @throws IOException 读取失败则抛出异常。
     */
    public static Stream<Row> stream(@NotNull FileSystem fs, @NotNull Path path) throws IOException {
        ORCReader reader = ORCReader.read(fs, path);
        return reader.stream();
    }

    public static ORCReader read(@NotNull FileSystem fs, @NotNull Path path) {
        return new ORCReader(fs, path);
    }


    private FileSystem      fs;
    private Path            path;
    private LinkedList<ORC> cache;
    private Schema[]        fields;

    private ORCReader(FileSystem fs, Path path) {
        this.fs   = fs;
        this.path = path;
        this.cache = new LinkedList<>();
        try {
            ORC orc = open0();
            this.fields = orc.row.header();
            orc.close();
        } catch (IOException e) {
            throw new IllegalArgumentException("Can not read MetaData from ORC File.", e);
        }
    }

    private ORC open0() throws IOException {
        // 打开文件
        ORC orc = new ORC();
        orc.open(fs, path);
        cache.add(orc);
        return orc;
    }

    @Override
    public Schema[] header() {
        return fields;
    }

    /**
     * 生成数据流
     * @return 返回数据数
     * @throws IOException 读取错误则抛出该异常。
     */
    public synchronized Stream<Row> stream() throws IOException {
        // 打开文件
        ORC orc = open0();
        // 构造迭代器
        Iterator<Row> iter = new Itr(orc);
        // 生成流
        Stream<Row> stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                iter, Spliterator.ORDERED | Spliterator.NONNULL), false);
        stream = stream.onClose(orc::close);
        //stream = stream.onClose(() -> {this.close(); orc.close();});
        return stream;
    }

    @Override
    public void close() {
        // help GC
        while(!cache.isEmpty()) {
            ORC orc = cache.remove(0);
            if (orc != null)
                orc.close();
        }
        fs   = null;
        path = null;
    }

    //==============================================================================================================
    // ORC 读取操作元素
    private final class ORC implements Closeable {
        private DataRow row;
        private TypeDescription       schema;
        private List<TypeDescription> children;
        private int                   fields;
        private Reader                reader;
        private RecordReader          records;
        //private StructObjectInspector inspector;
        private VectorizedRowBatch    batch;

        private void open(FileSystem fs, Path path) throws IOException {
            reader    = OrcFile.createReader(fs, path);
            records   = reader.rows();
            schema    = reader.getSchema();
            //inspector = (StructObjectInspector) reader.getObjectInspector();
            batch     = schema.createRowBatch();
            children  = schema.getChildren();
            fields    = children.size();
            // 解析元数据
            List<String> names             = schema.getFieldNames();
            List<TypeDescription> children = schema.getChildren();
            //List fields                    = orc.inspector.getAllStructFieldRefs();
            //((StructField) fields.get(0)).getFieldObjectInspector().getCategory();
            // 重组字段
            Schema[] fields = IntStream.range(0, names.size()).mapToObj(i -> {
                TypeDescription column = children.get(i);
                return new Column(names.get(i),column.getCategory().getName(), column.getScale(), column.getPrecision());
            }).toArray(Schema[]::new);
            // 构造 DataRow
            row = new DataRow(fields);
        }

        @Override
        public void close() {
            // help GC
            row       = null;
            batch     = null;
            reader    = null;
            records   = null;
            schema    = null;
            children  = null;
            //inspector = null;
        }
    }

    private final class Itr implements Iterator<Row> {
        private ORC      orc;
        private Object[] values;
        private int      cursor;

        Itr(ORC orc) {
            this.orc    = orc;
            this.values = new Object[orc.fields];
        }

        @Override
        protected void finalize() {
            orc    = null;
            values = null;
        }

        // 处理异常
        private void handleException() {
            if (orc != null)
               orc.close();
        }

        @Override
        public boolean hasNext(){
            try {
                boolean result;
                if (orc.batch.size == 0) {
                    result = orc.records.nextBatch(orc.batch);
                    cursor = 0;
                } else {
                    result = orc.batch.size > 0;
                }
                // 当已经没有了结果时，尝试关闭 ORC
                if (!result)
                    handleException();
                return result;
            } catch (IOException e) {
                handleException();
                throw new UncheckedIOException(e);
            }

            /*try {
                return orc.records.hasNext();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }*/
        }

        public Row next(){
            if (hasNext()) {
                // 每次 next 都会使 orc.batch.size 减 1
                orc.batch.size--;
                // 记录游标
                int rowCount = cursor++;
                for (int i = 0; i < orc.fields; i++) {
                    TypeDescription field = orc.children.get(i);
                    switch(field.getCategory()) {
                        case FLOAT:
                        case DOUBLE: values[i] = ((DoubleColumnVector) orc.batch.cols[i]).vector[rowCount]; break;
                        case BOOLEAN:
                        case INT:
                        case LONG:
                        case SHORT: values[i] = ((LongColumnVector) orc.batch.cols[i]).vector[rowCount]; break;
                        default: values[i] = ((BytesColumnVector) orc.batch.cols[i]).toString(rowCount); break;
                    }
                }
                orc.row.parse(values);
                return orc.row;
            } else {
                handleException();
                throw new NoSuchElementException();
            }
        }

        //==============================================================================================================
    }
}
