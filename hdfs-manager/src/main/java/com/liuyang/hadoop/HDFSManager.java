package com.liuyang.hadoop;

import com.liuyang.common.ManagerClient;
import com.liuyang.ds.Record;
import com.liuyang.ds.Row;
import com.liuyang.ds.Schema;
import com.liuyang.hadoop.data.*;
import com.liuyang.jdbc.hive.HiveTable;
import com.liuyang.log.Logger;
import com.liuyang.tools.IOUtils;
import com.liuyang.util.LinkedList;
import com.sun.istack.internal.NotNull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

public final class HDFSManager implements ManagerClient, AutoCloseable {
    private static Logger logger = Logger.getLogger(HDFSManager.class);

    // 合并流数据
    private static <R> Stream<R> concat(Stream<R> a, Stream<R> b) {
        if (a == null)
            return b;
        if (b == null)
            return a;
        return Stream.concat(a, b);
    }

    private Configuration hadoopConfig;
    private HDFSConfig    conf;
    private boolean       remote = true;
    private FileSystem    fs1; // remote file system
    private FileSystem    fs2; // local file system

    public HDFSManager() {
        hadoopConfig = new Configuration(true);
        hadoopConfig.set("dfs.replication", "4");
        hadoopConfig.set("dfs.support.append.broken", "true"); // 支持文件追加, 需要在集群上设置
        hadoopConfig.set("dfs.support.append", "true");        // 支持文件追加, 需要在集群上设置
        hadoopConfig.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        hadoopConfig.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        // HDFS 分布式文件系统实现类
        hadoopConfig.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        hadoopConfig.set("dfs.client.use.datanode.hostname", "true");
    }



    public HDFSManager(@NotNull HDFSConfig conf) {
        this.conf = conf;
    }

    private void syncConfig() {
        if (conf != null) {
            // true 表示可以从根目录中读取 core-site.xml, hdfs-site.xml 等配置文件。
            hadoopConfig = new Configuration(true);
            hadoopConfig.set("fs.defaultFS", conf.toString());
            hadoopConfig.set("hadoop.job.user", conf.getUser());
            hadoopConfig.set("hadoop.job.pass", conf.getPass());
            //this.uri = FileSystem.getDefaultUri(conf);
        }
    }

    /**
     * 获取文件系统。
     * <p>
     *     文件系统实例不需要调用 close 方法。
     * </p>
     * @param remote 是否远程
     * @return 返回文件系统实例对像。
     * @throws IOException 获取失败时抛出该异常。
     */
    private synchronized FileSystem getFileSystem(boolean remote) throws IOException {
       // return (remote ? (fs1 = FileSystem.get(hadoopConfig)) : (fs2 = FileSystem.getLocal(hadoopConfig)));
        if (remote) {
            return (fs1 == null ? fs1 = FileSystem.get(hadoopConfig) : fs1);
        } else {
            return (fs2 == null ? fs2 = FileSystem.getLocal(hadoopConfig) : fs2);
        }
    }

    public FileSystem getFileSystem() throws IOException {
        return getFileSystem(remote);
    }

    @Override
    public void close() {
        /*try {
            if (fs1 != null)
                fs1.close();
            if (fs2 != null)
                fs2.close();
        } catch (IOException e) {
            e.printStackTrace();
        }*/
        fs1 = null;
        fs2 = null;
    }


    public boolean connect() throws HDFSException {
        if (conf == null)
            return false;
        try {
            fs1 = FileSystem.get(hadoopConfig);
            fs2 = FileSystem.getLocal(hadoopConfig);
            return fs1 != null && fs2 != null;
        } catch (IOException e) {
            //e.printStackTrace();
            throw new HDFSException(e.getMessage(), e);
        }
    }

    public boolean connect(@NotNull HDFSConfig conf) throws HDFSException{
        this.conf = conf;
        syncConfig();
        return connect();
    }

    public boolean delete(@NotNull Path path, boolean recursive, boolean remote) {
        try(FileSystem fs = getFileSystem(remote)) {
            return fs.delete(path, recursive);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean delete(@NotNull Path path, boolean remote) {
        return delete(path, true, remote);
    }

    public HDFSManager enableRemote() {
        this.remote = true;
        return this;
    }

    public HDFSManager enableLocal() {
        this.remote = false;
        return this;
    }

    public final synchronized boolean exists(@NotNull Path path) throws IOException {
        FileSystem fs = getFileSystem();
        return fs.exists(path);
    }


    public HDFSConfig getConf() {
        return conf;
    }

    public long getLastConnectionTime() {
        return 0;
    }


    /**
     * 下载远程文件
     * @param local 指定本地文件
     * @param remote 指定元程文件
     * @param append 是否追加数据到本地文件末尾
     * @return 返回传输数据字节数。
     */
    public long get(@NotNull File local, @NotNull Path remote, boolean append) {
        try(FileSystem fs = getFileSystem();
            FileOutputStream out = new FileOutputStream(local, append);
            FSDataInputStream in = fs.open(remote)) {
            return IOUtils.copy(in, out, 1024, -1, false);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public boolean isConnected() {
        return fs1 != null && fs2 != null && conf != null;
    }

    /**
     * 是否使用本地文件系统
     * @return 返回 true 表示使用本地文件系统。
     */
    public boolean isLocal() {
        return !remote;
    }

    /**
     * 是否使用远程分布式文件系统
     * @return 返回 true 表示使用远程分布式文件系统。
     */
    public boolean isRemote() {
        return remote;
    }


    public final synchronized FileStatus[] list(@NotNull Path path) throws IOException {
        FileSystem fs = getFileSystem();
        return fs.listStatus(path);
    }

    /**
     * 上传本地文件
     * @param local 指定本地文件
     * @param remote 指定远程文件
     * @param append 是否追加到远程文件末尾。
     * @return 返回传输数据字节数。
     */
    public final synchronized long put(@NotNull File local, @NotNull Path remote, boolean append) throws  IOException {
        FileSystem fs = getFileSystem();
        FileInputStream in = new FileInputStream(local);
        FSDataOutputStream out = append ? fs.append(remote) : fs.create(remote, true);
        return IOUtils.copy(in, out, 1024, -1, false);
    }

    private Stream<Row> openStream(@NotNull Path path, @NotNull FileType storeType,
                                   // delimiter 和 fields 参数可选
                                   String delimiter, Schema[] fields) throws IOException{
        switch(storeType) {
            case ORC:      return readORC(path);
            case TEXTFILE: return readText(path, delimiter, fields);
            default:       return readText(path, delimiter, fields);
        }
    }

    public Stream<Row> readDataFromDirectory(@NotNull Path directory, @NotNull FileType storeType,
                                             // delimiter 和 fields 参数可选
                                             String delimiter, Schema[] fields) throws IOException {
        Stream<Row>  stream   = null;
        for (FileStatus status : list(directory)) {
            if (status.isFile()) {
                stream = concat(stream, openStream(status.getPath(), storeType, delimiter, fields));
            }
        }
        return stream;
    }


    public Stream<Row> readDataFromHiveTable(@NotNull HiveTable table) throws IOException {
        Path location = new Path(table.getLocation());
        String delimiter = table.getDelimiter();
        Schema[] fields  = table.getFields();
        FileType store   = FileType.find(table.getStoreType());
        return readDataFromDirectory(location, store, delimiter, fields);
    }

    /**
     * 读取 ORC 文件
     * @param path 指定文件
     * @return 返回数据流
     * @throws IOException 读取失败时抛出异常。
     */
    public Stream<Row> readORC(@NotNull Path path) throws IOException{
        return ORCReader.stream(getFileSystem(), path);
    }

    /**
     * 读取文本文件数据
     * @param path 指定文件路径
     * @param delimiter 数据分隔符号
     * @param fields 数据字段
     * @return 返回数据流
     * @throws IOException 读取失败时抛出异常。
     */
    public Stream<Row> readText(@NotNull Path path, String delimiter, @NotNull Schema[] fields) throws IOException {
        FileSystem fs = getFileSystem();
        TextReader reader = new TextReader(fs, path, fields, delimiter);
        return reader.stream();
    }

    /**
     * 将数据流写入 ORC 文件
     * @param path 指定 ORC 文件路径
     * @param append 是否在 ORC 文件末尾追加数据
     * @param stream 数据流
     */
    public void writeORC(@NotNull Path path, boolean append, @NotNull Stream<Row> stream) throws IOException {
        FileSystem fs = getFileSystem();
        // 将流数据写入文件
        try (ORCWriter writer = ORCWriter.create(fs, path, append)) {
            writer.write(stream);
        }

    }

    public void writeText(@NotNull Path path, boolean append,
                           @NotNull Schema[] fields, @NotNull Stream<Row> stream) throws IOException  {
        FileSystem fs = getFileSystem();
        try (TextWriter writer = TextWriter.create(fs, path, append)) {
            writer.write(stream);
        }
    }

}
