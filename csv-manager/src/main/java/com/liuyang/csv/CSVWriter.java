package com.liuyang.csv;

import com.liuyang.ds.Row;
import com.liuyang.ds.Schema;
import com.liuyang.ds.sets.TextRow;
import com.sun.istack.internal.NotNull;

import java.io.*;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * CSV 写入器
 * <ul>
 *     <li>2019/1/21 ver 1.0.0 创建。</li>
 * </ul>
 * @author liuyang
 * @version 1.0.1
 */
public class CSVWriter implements Closeable {

    public static CSVWriter create(@NotNull File csvFile, boolean append, Schema[] header){
        //if (!csvFile.isFile())
        //    throw new IllegalArgumentException(csvFile + " is not a file.");
        return new CSVWriter(csvFile, append, header);
    }

    private Stream<Row>    stream = null;
    private BufferedWriter writer = null;
    private TextRow        row    = null;
    private Schema[]       header = null;

    private boolean containsHeader;
    private String delimiter = ",";
    private String lineSeparator = "\n";
    private char leftQuotationMark = '"';
    private char rightQuotationMark = '"';
    private boolean append;
    private boolean isClosed = true;
    private long limit = 0;

    private File source;

    private CSVWriter(File source, boolean append, Schema[] header) {
        this.source = source;
        this.header = header;
        this.append = append;
        //System.lineSeparator().
        //this.row    = new TextRow()
    }

    @Override
    public void close() {
        try {
            if (writer != null) {
                writer.flush();
                writer.close();
            }
        } catch (IOException e) {
            // do nothing
        } finally {
            writer = null;
            isClosed = true;
        }
    }

    private void reset() {
        close();
    }

    private void handleException(Exception e) {
        e.printStackTrace();
        // 出现异常，重置流
        reset();
    }

    private void open() throws IOException {
        if (writer == null) {
            writer = new BufferedWriter(new FileWriter(source, append));
        }
    }

    public void write(Object[] arr) {
        if (arr == null)
            return;
        try {
            open();
            String[] row = Arrays.stream(arr).map(String::valueOf).toArray(n -> new String[n]);
            String content = String.join(delimiter, row);
            writer.write(content);
            writer.write(lineSeparator);
            //System.out.println("CSVWriter.write: " + content);
        } catch (IOException e) {
            handleException(e);
        }
    }

    public void write(String[] arr) {
        if (arr == null)
            return;
        try {
            open();
            String content = String.join(delimiter, arr);
            writer.write(content);
            writer.write(lineSeparator);
            //System.out.println("CSVWriter.write: " + content);
        } catch (IOException e) {
            handleException(e);
        }
    }
}
