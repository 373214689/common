package com.liuyang.hadoop.data;

import com.liuyang.ds.DataRecord;
import com.liuyang.ds.Row;
import com.sun.istack.NotNull;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.stream.Stream;

public class TextWriter implements Closeable {

    public static TextWriter create(FileSystem fs, Path path, boolean append) {
        return new TextWriter(fs, path, append);
    }

    private FileSystem fs;
    private Path       path;
    private boolean    append;
    private String     delimiter;
    private TXT        txt;

    private TextWriter(@NotNull FileSystem fs, @NotNull Path path, boolean append) {
        this.fs = fs;
        this.path = path;
        this.append = append;
    }

    public String getDelimiter(String delimiter) {
        return delimiter;
    }

    public TextWriter setDelimiter(String delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    @Override
    public void close() {
        if (txt != null)
            txt.close();
        // help GC
        fs        = null;
        path      = null;
        delimiter = null;
        txt       = null;
    }

    private TXT open0() throws IOException {
        TXT txt = new TXT();
        txt.open(fs, path, append);
        return txt;
    }

    public void write(@NotNull DataRecord<? extends Row> records) throws IOException {
        try (DataRecord<? extends Row> temp = records) {
            write(temp.stream());
        } catch (Exception e) {
            throw new IOException("Open DataRecord failure.", e);
        }
    }

    public void write(@NotNull Stream<? extends Row> stream) throws IOException {
        if (txt == null) {
            txt = open0();
        }
        stream.forEach(txt::write);
    }

    private class TXT implements Closeable {
        BufferedWriter out;

        public void close() {
            try {
                if (out != null)
                    out.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                out = null;
            }
        }

        private void open(FileSystem fs, Path path, boolean append) throws IOException {
            FSDataOutputStream fos = append ? fs.append(path) : fs.create(path, true);
            this.out = new BufferedWriter(new OutputStreamWriter(fos));
        }


        private void write(Row row){
            if (out != null) {
                try {
                    out.write(row.toString(delimiter));
                } catch (IOException e) {
                    // e.printStackTrace();
                }
            }
        }
    }
}
