package com.liuyang.hadoop.data;

import com.liuyang.ds.DataRecord;
import com.liuyang.ds.Row;
import com.liuyang.ds.Schema;
import com.liuyang.ds.sets.TextRow;
import com.liuyang.tools.StringUtils;
import com.liuyang.util.LinkedList;
import com.sun.istack.internal.NotNull;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Stream;

/**
 * Text 文件读取
 *
 * @author liuyang
 * @version 1.0.1
 */
public class TextReader implements DataRecord<Row>, Closeable {
    //
    private Path            path;
    private FileSystem      fs;
    private Schema[]        fields;
    private String          delimiter;
    //
    private LinkedList<TXT> cache;

    public TextReader(@NotNull FileSystem fs, @NotNull Path remote,
                      @NotNull Schema[] fields, @NotNull String delimiter) {
        this.fs       = fs;
        this.path      = remote;
        this.fields    = fields;
        this.delimiter = delimiter;
        this.cache     = new LinkedList<>();
    }

    @Override
    public final void close() {
        while(!cache.isEmpty()) {
            TXT txt = cache.remove(0);
            if (txt != null)
                txt.close();
        }
        // help GC
        fs        = null;
        cache     = null;
        fields    = null;
        delimiter = null;
    }

    //public String getDelimiter() {
    //    return delimiter;
    //}

    @Override
    public Schema[] header() {
        return fields;
    }

    private TXT open0() throws IOException {
        TXT txt = new TXT();
        txt.open(fs, path);
        cache.add(txt);
        return txt;
    }

    public synchronized Stream<Row> stream() throws IOException {
        // 读取文本文件
        TXT txt = open0();
        Stream<Row> stream = txt.in.lines().map(s ->  txt.row.parse(StringUtils.split(s, delimiter)));
        stream = stream.onClose(txt::close);
        return stream;
    }


    private class TXT implements Closeable {
        private BufferedReader in;
        private TextRow        row;

        TXT() {
        }

        private void open(FileSystem fs, Path path) throws IOException {
            this.in  = new BufferedReader(new InputStreamReader(fs.open(path)));
            this.row = new TextRow(fields);
        }

        public void close() {
            try {
                if (in != null)
                    in.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                // help GC
                in   = null;
                row  = null;

            }
        }


    }
}
