package com.liuyang.hadoop.data;

import com.liuyang.ds.DataRecord;
import com.liuyang.ds.Row;
import com.liuyang.ds.Schema;
import com.sun.istack.NotNull;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.WriterOptions;
import org.apache.orc.TypeDescription;


import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

/**
 * ORC 文件写入
 *
 */
public class ORCWriter implements Closeable {

    public static ORCWriter create(FileSystem fs, Path path, boolean append) {
        return new ORCWriter(fs, path, append);
    }

    public static TypeDescription createStruct(Schema[] fields) {
        TypeDescription struct = TypeDescription.createStruct();
        for(Schema field : fields) {
            String name      = field.getName();
            int    scale     = field.getScale();
            int    precision = field.getPrecision();
            TypeDescription schema;
            switch(field.getType()) {
                case SHORT    : schema = TypeDescription.createShort(); break;
                case TINYINT  : schema = TypeDescription.createShort(); break;
                case SMALLINT : schema = TypeDescription.createShort(); break;
                case INTEGER  : schema = TypeDescription.createInt(); break;
                case INT      : schema = TypeDescription.createInt(); break;
                case BIGINT   : schema = TypeDescription.createLong(); break;
                case LONG     : schema = TypeDescription.createLong(); break;
                case DOUBLE   : schema = TypeDescription.createDouble(); break;
                case FLOAT    : schema = TypeDescription.createFloat(); break;
                case STRING   : schema = TypeDescription.createString(); break;
                default: schema = TypeDescription.createString(); break;
            }
            //schema.withScale(scale).withPrecision(precision);
            struct.addField(name, schema);
        }
        return struct;
    }

    //FileType
    private FileSystem      fs;
    private Path            path;
    private ORC             orc;
    private boolean         append;

    private ORCWriter(FileSystem fs, Path path, boolean append) {
        this.fs     = fs;
        this.path   = path;
        this.append = append;
    }

    @Override
    public void close() {
        if (orc != null)
            orc.close();
        // help GC
        fs   = null;
        orc  = null;
        path = null;
    }

    /**
     * 将记录中的数据写入 ORC 文件
     * @param records 指定数据记录。
     * @throws IOException 写入出错时抛出该异常。
     */
    public void write(@NotNull DataRecord<? extends Row> records) throws IOException{
        try (DataRecord<? extends Row> temp = records) {
            write(temp.stream());
        } catch (Exception e) {
            throw new IOException("Open DataRecord failure.", e);
        }
    }

    /**
     * 将指定的数据流写入 ORC 文件。
     * <p>
     *     如果在构造时设置 append = true，则在文件末尾追加数据。
     *     由于 ORC 原本不支持追加操作，所以此处的追加是将原文件重建后，再在末尾写入新的数据。
     * </p>
     * <p>
     *     该操作不会关闭 ORC 文件的操作，如果长期空闲，建议执行 close，以避免消耗 IO 性能。
     * </p>
     * @param stream 指定数据流。
     */
    public void write(@NotNull Stream<? extends Row> stream){
        if (orc == null) {
            orc = new ORC();
        }
        stream.forEach(orc::write);
    }



    // ORC 操作元素
    private final class ORC implements Closeable {
        private volatile Path                  tmp;
        private volatile Writer                writer;
        private volatile WriterOptions         options;
        private volatile TypeDescription       struct;
        private volatile VectorizedRowBatch    batch;
        private volatile List<TypeDescription> children;

        ORC() {

        }

        // 初始化。准备文件
        private void open(FileSystem fs, Path path, boolean append, Schema[] fields) throws IOException {
            options = OrcFile.writerOptions(fs.getConf());
            options.stripeSize(67108864);
            options.bufferSize(131072);
            options.blockSize(134217728);
            options.compress(CompressionKind.SNAPPY);
            options.version(OrcFile.Version.V_0_12);
            options.fileSystem(fs);
            struct   = createStruct(fields);
            children = struct.getChildren();
            batch    = struct.createRowBatch(8192);
            options.setSchema(struct);
            if (!fs.exists(path)) {
                writer = OrcFile.createWriter(path, options);
                //System.out.println("追加文件【原文件不存在】");
            } else {
                // 检查是否追加文件
                if (append) {
                    tmp = new Path(path.getParent(),
                            String.format("%s_%06x.tmp", path.getName(), System.nanoTime() % 65535));
                    fs.rename(path, tmp);
                    writer = OrcFile.createWriter(path, options);
                    ORCWriter.this.write(ORCReader.read(fs, tmp));
                } else {
                    fs.delete(path, true);
                    writer = OrcFile.createWriter(path, options);
                    //System.out.println("追加文件【原文件存在】");
                }
            }
        }

        // 配置
        private void config(Row row) {
            if (struct == null || writer == null) {
                try {
                    open(fs, path, append, row.header());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // 解析数据
        private void write(Row row){
            config(row);
            //System.err.println(row);
            //System.err.println("----------------------------" + (batch == null || writer == null) + "----------------------------");
            if (batch == null || writer == null)
                return;
            try {
                int rowCount = batch.size++;
                for(int i = 0; i < batch.numCols; i++) {
                    ColumnVector vector = batch.cols[i];
                    switch(children.get(i).getCategory()) {
                        case SHORT:
                        case INT:
                        case LONG:   ((LongColumnVector) vector).vector[rowCount] = row.getLong(i); break;
                        case FLOAT:
                        case DOUBLE: ((DoubleColumnVector) vector).vector[rowCount] = row.getDouble(i); break;
                        case BINARY:
                        case STRING:
                        default:     ((BytesColumnVector) vector).setVal(rowCount, row.getBinary(i));
                    }
                }
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            } catch (IOException e) {
                // 发生异常时，回退一行
                batch.size--;
                e.printStackTrace();
            }
        }

        @Override
        public void close() {
            try {
                // System.out.println("try close ORC writer.");
                if (batch != null)
                    writer.addRowBatch(batch);
                // 将文件写入磁盘
                if (writer != null)
                    writer.close();
                // 删除临时文件
                Path crc = new Path(path.getParent(), "." + path.getName() + ".crc");
                if (fs.exists(crc))
                    fs.delete(crc, true);
                if (tmp != null)
                    fs.delete(tmp, true);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                writer   = null;
                struct   = null;
                batch    = null;
                children = null;
            }

        }
    }
}
