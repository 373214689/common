package com.liuyang.csv;

import com.liuyang.ds.DataSet;
import com.liuyang.ds.Row;
import com.liuyang.ds.Schema;
import com.liuyang.ds.Type;
import com.liuyang.ds.attr.Column;
import com.liuyang.ds.sets.TextRow;
import com.liuyang.tools.StringUtils;
import com.sun.istack.internal.NotNull;

import java.io.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * CSV 读取器
 * <ul>
 *     <li>2019/1/21 ver 1.0.0 创建。</li>
 *     <li>2019/1/22 ver 1.0.1 将 Row 替换为 CSVRecord 操作。</li>
 * </ul>
 * @author liuyang
 * @version 1.0.1
 */
public final class CSVReader implements DataSet, Closeable {

    public static CSVReader open(@NotNull File csvFile, boolean containsHeader) {
        if (!csvFile.exists())
            throw new IllegalArgumentException(csvFile + " has not been found.");
        if (!csvFile.isFile())
            throw new IllegalArgumentException(csvFile + " is not a file.");
        return new CSVReader(csvFile, containsHeader);
    }

    public static CSVReader open(@NotNull File csvFile, Schema[] header) {
        if (!csvFile.exists())
            throw new IllegalArgumentException(csvFile + " has not been found.");
        if (!csvFile.isFile())
            throw new IllegalArgumentException(csvFile + " is not a file.");
        return new CSVReader(csvFile, header);
    }

    public static String readAll(File csvFile) {
        try (FileReader reader = new FileReader(csvFile)) {
            StringBuilder builder = new StringBuilder();
            char[] buff = new char[1024];
            int len;
            while ((len = reader.read(buff)) != -1)
                builder.append(buff, 0, len);
            return builder.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Stream<CSVRecord> stream = null;
    private BufferedReader    reader = null;
    private TextRow           row    = null;
    private Schema[]          header = null;

    private boolean containsHeader;
    private String delimiter = ",";
    private char leftQuotationMark = '"';
    private char rightQuotationMark = '"';
    private boolean isClosed = true;
    private boolean parallel = false;
    private long limit = 0;

    private File source;

    private CSVReader(@NotNull File source, boolean containsHeader) {
        this.source = source;
        this.containsHeader = containsHeader;
    }

    private CSVReader(@NotNull File source, @NotNull Schema[] header) {
        this.source = source;
        this.header = header;
    }

    private Schema[] parseHeader(@NotNull String line) {
        Schema [] fields = null;
        String[] fieldNames;
        fieldNames = StringUtils.split(line, delimiter);
        fields = new Column[fieldNames.length];
        for(int i = 0, length = fieldNames.length; i < length; i++) {
            fields[i] = new Column(fieldNames[i], Type.STRING, 0, 0);
        }
        return fields;
    }

    private Schema[] parseHeader() {
        try {
            read();
        }  catch (IOException e) {
            e.printStackTrace();
        }
        BufferedReader reader = null;
        Schema [] fields = null;
        String[] fieldNames;
        try {
            //com.mysql.jdbc.StringUtils
            reader = new BufferedReader(new FileReader(source));
            fieldNames = StringUtils.split(reader.readLine(), delimiter);
            fields = new Column[fieldNames.length];
            for(int i = 0, length = fieldNames.length; i < length; i++) {
                fields[i] = new Column(fieldNames[i], Type.STRING, 0, 0);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
                // do nothing
            } finally {
                reader = null;
                fieldNames = null;
            }
        }
        return fields;
    }


    private void reset() {
        close();
    }

    private void handleException(Exception e) {
        e.printStackTrace();
        // 出现异常，重置流
        reset();
    }

    private CSVRecord createRecord(String text) {
        row.parse(StringUtils.split(text, delimiter));
        return new CSVRecordImpl(row.getModifiedCount(), text, row);
    }

    private void read() throws IOException {
        if (reader == null || stream == null) {
            reader = new BufferedReader(new FileReader(source));
            isClosed = false;
            if (containsHeader)
                header = parseHeader(reader.readLine());
            row = new TextRow(header);
            //stream.collect(Collectors.groupingBy(row -> row.toMap())).
            if (parallel)
                stream = reader.lines().parallel().map(this::createRecord);
            else
                stream = reader.lines().map(this::createRecord);
            stream = stream.onClose(this::close);
        }
    }

    public long count() {
        long result = -1;
        try {
            read();
            if (limit > 0)
                result = stream.limit(limit).count();
            else
                result = stream.count();
        } catch (IOException e) {
            handleException(e);
        } finally {
            // 输出结果后，重置流
            reset();
        }
        return result;
    }

    @Override
    public void close() {
        try {
            if (reader != null)
                reader.close();
            if (stream != null)
                stream.close();
        } catch (IOException e) {
            // do nothing
        } finally {
            limit  = 0;
            reader = null;
            stream = null;
            isClosed = true;
            parallel = false;
        }
    }

    //public <R, A> R collect(Collector<? super Row, A, R> collector);
    public DataSet distinct() {
        try {
            read();
            stream = stream.distinct();
        } catch (IOException e) {
            handleException(e);
        } finally {
            // do nothing
        }
        return this;
    }

    public Schema[] header() {
        return header;
    }

    public void lines(Consumer<String> action) {
        try (BufferedReader reader = new BufferedReader(new FileReader(source))){
            Stream<String> stream = parallel ? reader.lines().parallel() : reader.lines();
            stream = limit > 0 ? stream.limit(limit) : stream;
            stream.forEach(action);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            reset(); // 重置
        }
    }

    public DataSet filter(Predicate<CSVRecord> filter){
        try {
            read();
            stream = stream.filter(filter);
        } catch (IOException e) {
            handleException(e);
        }
        return this;
    }

    public void forEach(Consumer<CSVRecord> action) {
        try {
            read();
            (limit > 0 ? stream.limit(limit) : stream).forEach(action);
        } catch (IOException e) {
            handleException(e);
        } finally {
            // 输出结果后，重置流
            reset();
        }
    }

    //private void action(Row)

    public <R> Stream<R> map(Function<CSVRecord, ? extends R> mapper){
        try {
            read();
            return stream.map(mapper);
        } catch (IOException e) {
            handleException(e);
        }
        return null;
    }

    public DataSet sorted() {
        try {
            read();
            stream = stream.sorted();
        } catch (IOException e) {
            handleException(e);
        } finally {
            // do nothing
        }
        return this;
    }

    public final CSVReader take(long num) {
        this.limit = num;
        return this;
    }

    @Override
    public final Stream<Row> stream() {
        try {
            read();
            //stream.count();
        } catch (IOException e) {
            // do nothing
            //handleException(e);
            e.printStackTrace();
        }
        return stream.map(CSVRecord::getRow);
    }

    public final CSVReader parallel() {
        parallel = true;
        return this;
    }

    public CSVReader setDelimiter(String delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    public CSVReader setQuotationMark(char left, char right) {
        this.leftQuotationMark = left;
        this.rightQuotationMark = right;
        return this;
    }

    // CSV Record
    private final static class CSVRecordImpl implements CSVRecord {
        private long   index;
        private int   bytes;
        private String text;
        private Row    row;

        public CSVRecordImpl(long index, String text, Row row) {
            this.index = index;
            this.text  = text;
            this.row   = row;
            this.bytes = StringUtils.bytes(text);
        }

        public long getIndex() {
            return index;
        }

        public int getBytes() {
            return bytes;
        }

        public String getText() {
            return text;
        }

        public Row getRow() {
            return row;
        }
    }
}
