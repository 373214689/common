package com.liuyang.ftp.client;

import com.liuyang.common.ManagerClient;
import com.liuyang.common.ManagerClientMonitor;
import com.liuyang.ftp.*;
import com.liuyang.log.Logger;
import com.liuyang.tools.StringUtils;
import com.liuyang.tools.TimeUtils;
import com.sun.istack.internal.NotNull;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.function.LongFunction;
import java.util.function.Predicate;

/**
 * FTP 客户端
 *
 * <p><b>FTP 命令列表如下：</b></p>
 * <p>
 * ABOR ACCT ALLO APPE CDUP CWD  DELE EPRT EPSV FEAT HELP LIST MDTM MKD
 * MODE NLST NOOP OPTS PASS PASV PORT PWD  QUIT REIN REST RETR RMD  RNFR
 * RNTO SITE SIZE SMNT STAT STOR STOU STRU SYST TYPE USER XCUP XCWD XMKD
 * XPWD XRMD
 * </p>
 * @version 1.0.0
 */
public class FTPClient implements ManagerClient, Closeable {
    private final static Logger logger = Logger.getLogger(FTPClient.class);

    // FTP 流模式
    private enum Mode {
        /** 追加追加文件数据：客户端需要能过输出流上传数据。 */
        APPEND,
        /** 打开远程文件：客户端需要通过输入流下载数据。 */
        OPEN,
        /** 覆盖远程文件：客户端需要能过输出流上传数据。  */
        OVERWRITE,
    }

    private BufferedReader reader = null;
    private BufferedWriter writer = null;
    private Socket         client = null;
    /** 服务器IP */
    private String         serverIP = null;
    private String         clientIP = null;
    /** 最后指令 */
    private String         command  = null;

    private FTPConfig conf;

    private int connTimeout = 3000;

    private boolean debug = true;
    private volatile long last = 0;


    FTPClient () {
        this.conf = new FTPConfig();
    }

    public FTPClient (URI uri) throws UnsupportedEncodingException {
        this.conf = new FTPConfig();
        this.conf.parseURI(uri);
    }

    public FTPClient (FTPConfig config) {
        if (config == null)
            throw new NullPointerException("the config parameter is null");
        this.conf = config;
    }

    public FTPClient(String host, int port, String user, String pass, String path) {
        this.conf = new FTPConfig(host, port, user, pass);
        this.conf.setPath(path);
    }

    private void checkConnection() throws FTPClientException {
        if (client == null)
            throw new FTPClientException("please create connection at first.");
        if (!isConnected())
            throw new FTPClientException("connection is not connected.");
    }

    // 接收服务器响应消息
    private synchronized FTPResponse recv() throws FTPClientException {
        checkConnection();
        String message;
        //logger.debug("try receive response");
        try {
            message = reader.readLine();
        } catch (IOException e) {
            throw new FTPClientException("Receive response from " + serverIP + " failure (previous command: "
                    + command + ")."
                    + e.getMessage(), e);
        }

        int    code;
        String text;
        // 如果解析出现异常，则进行处理
        try {
            code = Integer.parseInt(message.substring(0, 3));
            text = message.substring(4);
        } catch (Exception e) {
            code = 0;
            text = message;
        } finally {
            // 记录最后连接时间
            recordLastConnectionTime();
        }
        if (debug)
            logger.debug("[%s] recv >> code=%d, message=%s", serverIP, code, text);
        return new FTPResopnseImpl(code, text);
    }

    // 发送请求
    private synchronized FTPResponse send(String method, String... parameters) throws FTPClientException {
        checkConnection();
        //logger.debug("host=%s request: method=%s parameter=%s", host, method, String.join(",", parameters));
        try {
            if (debug)
                logger.debug("[%s] send >> method=%s, parameters=%s", clientIP, method, String.join(",", parameters));
            command = String.format("%s %s", method, String.join(" ", parameters));
            writer.write(command);
            writer.write("\r\n");
            writer.flush();
            // 记录最后连接时间
            recordLastConnectionTime();
        } catch (IOException e) {
            throw new FTPClientException("Send request to " + serverIP + "failure. (current command: "
                    + command + ")."
                    + e.getMessage(), e);
        }
        return recv();
    }

    private void recordLastConnectionTime() {
        last = System.currentTimeMillis();
    }

    // 建立传输通道
    private synchronized Socket channel() throws FTPClientException {
        checkConnection();
        if (conf.isActiveMode()) {
            throw new FTPClientException("Not support active mode.");
        } else {
            return PASV();
        }
    }

    // 通知服务中止以前的 FTP 命令和与之相关的数据传送。
    // 如果先前的操作已经完成，则没有动作，返回226。如果没有完成，返回426，然后再返回226。
    // 关闭控制连接，数据连接不关闭。
    private synchronized FTPResponse ABOR(String command) throws FTPClientException {
        return send("ABOR", command);
    }

    // 服务器文件追加。如果文件存在则会在其末尾追加数据，不存在则创建。
    // 该操作需要调用 PASV/PORT 将数据传入到服务器。
    private synchronized FTPResponse APPE(String path) throws FTPClientException {
        return send("APPE", path);
    }

    // 设置客户端当前工作路径
    private synchronized FTPResponse CWD(String path) throws FTPClientException {
        return send("CWD", path);
    }

    // 删除文件
    private synchronized FTPResponse DELE(String path) throws FTPClientException {
        return send("DELE", path);
    }

    // 获取帮助
    private synchronized String[] HELP(String command) throws FTPClientException {
        Socket conn = channel();
        FTPResponse resp;
        if (command == null)
            resp = send("HELP");
        else
            resp = send("HELP", command);
        String[] retval = new String[] {};
        if (!resp.getStatus()) {
            return retval;
        }
        while(true) {
            resp = recv();
            // 如果接收到 FTP 消息，则跳出循环。
            if (resp.getStatus())
                break;
            String[] words = resp.getText().trim().split(" ");
            int length = retval.length;
            // 动态改变数组
            retval = Arrays.copyOf(retval, length + words.length);
            System.arraycopy(words, 0, retval, length, words.length);
        }
        return retval;
    }

    // 获取文件时间
    private synchronized FTPResponse MDTM(String path) throws FTPClientException {
        return send("MDTM", path);
    }

    // 设置传输模式。参考以下模式代码：（FTP 服务器目前一般只支持 S 模式）。
    // S -- Stream Mode  （流模式）
    // B -- Block Mode   （块模式）
    // C -- Compress Mode（压缩模式
    private synchronized FTPResponse MODE(String mode) throws FTPClientException {
        return send("MODE", mode);
    }

    // 无操作
    private synchronized FTPResponse NOOP() throws FTPClientException {
        return send("NOOP");
    }

    // 设置某项操作：ON/OFF
    private synchronized FTPResponse OPTS(String opeartion, boolean flag) throws FTPClientException {
        return send("OPTS", opeartion, (flag ? "ON" : "OFF"));
    }

    // 输入密码
    private synchronized FTPResponse PASS(String pass) throws FTPClientException {
        return send("PASS", pass);
    }

    // 获取客户端工作路径
    private synchronized String PWD() throws FTPClientException {
        return send("PWD").getText().replace('"', '\0');
    }

    // 被动模式
    private synchronized Socket PASV() throws FTPClientException {
        FTPResponse resp = send("PASV");

        if (!resp.getStatus())
            throw new FTPClientException("can not open passive mode. " + resp.getText());

        String text = resp.getText();

        int pos1 = text.indexOf('(');
        int pos2 = text.lastIndexOf(')');
        String[] arrs = text.substring(pos1 + 1, pos2).split(",");
        int p1 = Integer.parseInt(arrs[4]);
        int p2 = Integer.parseInt(arrs[5]);
        int port = p1 * 256 + p2;

        String host = String.join(".", Arrays.copyOf(arrs, 4));
        try {
            Socket pasv = new Socket();
            pasv.connect(new InetSocketAddress(host, port), connTimeout);
            return pasv;
        } catch (IOException e) {
            throw new FTPClientException(e.getMessage());
        }
    }

    // 主动模式
    private synchronized Socket PORT() throws FTPClientException {
        return null;
    }

    // 设置偏移量
    // 该命令并不传送文件，而是略过指定点后的数据。此命令后应该跟其它要求文件传输的 FTP 命令。
    // 如：“REST 100\r\n”，重新指定文件传送的偏移量为 100 字节。
    private synchronized FTPResponse REST(int offset) throws FTPClientException {
        return send("REST", String.valueOf(offset));
    }

    // 重置连接
    // 重置后已有连接会断开。
    private synchronized FTPResponse REIN() throws FTPClientException {
        return send("REIN");
    }

    // 从服务器复制文件数据。该操作不会影响服务器已有的数据。
    // 需要使用 PASV/PORT 获取数据。
    private synchronized FTPResponse RETR(String path) throws FTPClientException {
        return send("RETR", path);
    }

    // 重命名，第一步：选中需要重命名的文件或目录
    private synchronized FTPResponse RNFR(String oldName) throws FTPClientException {
        return send("RNFR", oldName);
    }

    // 重命名，第二步：将被选中的文件和目录重命名
    private synchronized FTPResponse RNTO(String oldName, String newName) throws FTPClientException {
        return send("RNTO", oldName, newName);
    }

    // 获取路径对应文件的长度，如果路径不是文件，则返回 5xx 错误
    private synchronized long SIZE(String path) throws FTPClientException {
        FTPResponse resp = send("SIZE", path);
        if (resp.getStatus())
            return Long.parseLong(resp.getText());
        else
            return -1;
    }

    // 显示路径状态信息。
    // 最多获取目录下10个路径状态信息，超出部分，可以使用 LIST 命令。
    private synchronized String[] STAT(String path) throws FTPClientException {
        FTPResponse resp = send("STAT", path);
        String[] retval = new String[] {};
        if (!resp.getStatus()) {
            return retval;
        }
        int limit = 10;
        while(true) {
            resp = recv();
            if (resp.getStatus())
                break;
            if (limit > 0) {
                // 动态改变数组
                int length = retval.length;
                retval = Arrays.copyOf(retval, length + 1);
                retval[length] = resp.getText();
            }

            limit--;
        }
        if (limit <= 0)
            ABOR("STAT");
        if (limit <= 0)
            REIN();
        return retval;
    }

    // 上传文件到服务器，需要开启 PASV/PORT 写入文件数据。
    // 此操作中文件具有唯一性，如果服务器中已存在同名文件，则会被覆盖。
    private synchronized FTPResponse STOR(String path) throws FTPClientException {
        return send("STOR", path);
    }

    // 与 STOR 作用相同。
    // 区别在于如果服务器存在同名文件，则会进行重命名操作（如增加后缀：.1、.n等等），并不会覆盖原来的数据。
    private synchronized FTPResponse STOU(String path) throws FTPClientException {
        return send("STOU", path);
    }

    // 设置数据传输模式，可参考以下代码：（FTP 服务器一般默认为 I 模式）
    // A - ASCII 模式，即数据以字符形式传输。
    // E - EBCDIC 模式。
    // I - IMAGE 模式，也称为 BINARY （二进制）模式。
    // L - 设置本地字节长度，可以设置相应的数字。
    // 其中， A 和 E 后面还可以跟以下参数：（具体作用要看 FTP 服务器是否支持）
    //    T -- Telnet format effector
    //    C -- Carriage Control (ASA)
    private synchronized FTPResponse TYPE(String type) throws FTPClientException {
        return send("TYPE", type);
    }

    // 输入登陆用户名称。
    private synchronized FTPResponse USER(String user) throws FTPClientException {
        return send("USER", user);
    }

    // 在服务器上创建目录
    private synchronized FTPResponse XMKD(String user) throws FTPClientException {
        return send("XMKD", user);
    }

    // 在服务器上删除目录。
    // 需要确保所删除目录下没有文件或子目录。
    private synchronized FTPResponse XRMD(String user) throws FTPClientException {
        return send("XRMD", user);
    }


    @Override
    // close 方法不打印异常信息
    public void close() {
        try {
            if (client != null)
                client.close();
        } catch (IOException e) {
            // e.printStackTrace();
        }
    }

    @Override
    public synchronized boolean connect() throws FTPConnectException {
        // 如果存在连接，则需要关闭之前所建立的连接
        try {
            if (client != null) client.close();
        } catch (IOException e) {
            //e.printStackTrace();
        }
        // 尝试建立新的连接
        try {
            if (debug)
                logger.debug("conn >> host=%s, port=%d", conf.getHost(), conf.getPort());
            client = new Socket();
            //client.setSoTimeout(connTimeout);
            //client.connect(new InetSocketAddress(host, port), timeoutMills);
            client.connect(new InetSocketAddress(conf.getHost(), conf.getPort()), connTimeout);
            reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
            writer = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()));
            //
            serverIP = client.getInetAddress().getHostAddress();
            clientIP = client.getLocalAddress().getHostAddress();
            // 记录最后连接时间
            recordLastConnectionTime();
            // 接收欢迎消息
            recv();
            if (debug)
                logger.debug("[%s] connect successful.", serverIP);
            // 尝试登陆
            login();
            // 获取当前工作路径
            String current = PWD();
            // 如果配置中的服务器路径不为当前的工作路径，则跳转到配置中的服务器路径。
            if (!current.equals(conf.getPath()) && !StringUtils.isEmpty(conf.getPath())) {
                CWD(conf.getPath());
            }
        } catch (IOException e) {
            throw new FTPConnectException(e.getMessage());
        } catch (FTPClientException e) {
            e.printStackTrace();
        }
        return true;
    }

    // 删除文件
    public synchronized boolean deleteFile(String path) throws FTPClientException {
        return DELE(path).getStatus();
    }

    // 关闭调试信息
    public void disableDebug() {
        debug = false;
    }

    // 显示调试信息
    public void enableDeubg() {
        debug = true;
    }

    // 判断文件或路径是否存在
    public synchronized boolean exists(String path) throws FTPClientException {
        FTPResponse resp = send("STAT", path);
        if (!resp.getStatus()) {
            return false;
        }
        int result = 0;
        while(true) {
            resp = recv();
            if (resp.getStatus())
                break;
            result++;
        }
        return result > 0;

    }

    /**
     * 从服务器上下载文件到本地
     * @param path 服务器文件路径
     * @param local 本地文件路径，如果路径是一个目录，则会在目录中创建与服务器文件同名的文件名。
     * @param append 填入 true 表示在本地文件尾部追加数据， false 表示覆盖本地文件。
     * @param status 指定文件状态
     * @throws FTPClientException 传输过程中出错则抛出该异常。
     */
    public void get(@NotNull String path, @NotNull File local, boolean append,
                    @NotNull final FileStatus status) throws FTPClientException {
        open0(path, Mode.OPEN,  0, (in, out) -> {
            //ManagerClientMonitor monitor = ManagerClientMonitor.monitoring(this, connTimeout);
            try (FileOutputStream fout = new FileOutputStream(local, append)) {
                status.setStartTime();
                status.setLocalFile(local);
                byte[] buffer = new byte[1024];
                while (true) {
                    int len = in.read(buffer, 0, 1024);
                    if (len <= -1) break;
                    // 记录最后连接时间
                    recordLastConnectionTime();
                    fout.write(buffer, 0, len);
                    status.setLength(x -> x + (long) len);
                }
                fout.flush();
                status.setStatus(FileStatus.STATUS_RECEIVED);
                status.setEndTime();
            } catch (IOException e) {
                status.setStatus(FileStatus.STATUS_ABORT);
                throw new FTPClientException(e.getMessage());
            }
            return status.getLength();
        });
    }

    /**
     * 从服务器上下载文件到本地
     * @param path 服务器文件路径
     * @param local 本地文件路径，如果路径是一个目录，则会在目录中创建与服务器文件同名的文件名。
     * @param append 填入 true 表示在本地文件尾部追加数据， false 表示覆盖本地文件。
     * @return 执行成功后返回文件数据长度。
     * @throws FTPClientException 传输过程中出错则抛出该异常。
     */
    public FileStatus get(String path, File local, boolean append) throws FTPClientException {
        FileStatusImpl status = new FileStatusImpl(path);
        get(path, local, append, status);
        return status;
    }


    @Override
    public FTPConfig getConf() {
        return conf;
    }

    public String getCurrentWorkDirectory()throws FTPClientException{
        return PWD();
    }

    /**
     * 获取 FTP 远程命令列表
     * @return 返回命令列表。
     */
    public String[] getCommandHelp()throws FTPClientException{
        return HELP(null);
    }

    @Deprecated
    public String[] getCommandHelp(String command) throws FTPClientException{
        return HELP(command);
    }

    /**
     * 获取 FTP 服务器文件或路径信息，查看文件列表建议使用<code>listFile</code>。
     * <p>
     *     <i>该方法不适用大量文件信息的获取，建议使用 <code>listFile</code>.</i>
     * </p>
     * @param path 指写路径，如果是目录，则只能显示该目录下最多10个文件或路径信息。
     * @return 服务器文件或路径信息。
     */
    @Deprecated
    public synchronized FTPFile[] getStatus(String path)throws FTPClientException{
        String[] links = STAT(path);
        FTPFile[] retval = new FTPFile[links.length];

        for(int i = 0,length = links.length; i < length; i++) {
            retval[i] = FTPFile.parsex(path, links[i]);
        }
        return retval;
    }

    @Override
    public long getLastConnectionTime() {
        return last;
    }

    /**
     * 获取指定文件的最近修改时间
     * @param path 路径必须是文件（或文件链接）
     * @return 获取成功后返回文件时间，如果返回 null 表示获取失败（原因一般是路径不是文件）。
     */
    public synchronized Date getModifiedDate(String path) {
        FTPResponse resp = MDTM(path);
        if (resp.getStatus())
            return TimeUtils.parse("YYYYMMDDhhmmss", resp.getText());
        else
            return null;
    }

    /**
     * 获取 FTP 服务器状态信息
     * @return 返回服务器状态信息
     */
    public synchronized String getServerStatus()throws FTPClientException{
        FTPResponse resp = send("STAT");
        if (!resp.getStatus()) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        builder.append(resp.getText());
        while(true) {
            resp = recv();
            if (resp.getStatus())
                break;
            builder.append('\n').append(resp.getText());
        }
        builder.append('\n').append(resp.getText());
        return builder.toString();
    }

    // true 表示未超时， false 表示超时
    private boolean operationCheck() {
        return (System.currentTimeMillis() - last) < connTimeout;
    }

    @Override
    public boolean isConnected() {
        if (client == null) return false;
        // 如果最后操作间隔超时，则说明连接可能断开。
        if (operationCheck()) {
            return true;
        } else {
            // 如果 input 和 out 其中有一个关闭，则说明连接已断开。
            if (!(client.isInputShutdown() || client.isOutputShutdown()))
                return true;
            if (!client.isClosed())
                return true;
            if (client.isConnected())
                return true;
            return false;
        }
    }

    /**
     * 获取文件长度（单位：byte）。
     * @param path 路径必须是文件（或文件链接）。
     * @return 获取成功则返回文件字节长度，如果文件不存在或者路径不是文件，则返回 -1。
     */
    public synchronized long length(String path) {
        return SIZE(path);
    }

    // 按指定的 Mode 打开路径，Mode 包含追加、覆盖、读取等等。
    private synchronized long open0(String path, @NotNull Mode mode,
                                    int offset, FTPIOStream action) throws FTPClientException {
        // 启用被动或主动传输模式
        try (Socket conn = channel()){
            // 设置偏移量，如果小于或等于零，则不进行操作。
            if (offset > 0)
                REST(offset);
            FTPResponse resp = null;
            // 根据 mode 发送不同的指令
            switch(mode) {
                case APPEND:    resp = APPE(path); break;
                case OPEN:      resp = RETR(path); break;
                case OVERWRITE: resp = STOR(path); break;
            }
            if (!resp.getStatus())
                throw new FTPClientException(String.valueOf(mode).toLowerCase() + " " +
                    path + " failure: " + resp.getText() + ".");
            //String text = resp.getText();
            //int  pos1 = text.indexOf('(');
            //int  pos2 = text.indexOf(" bytes).");
            //long length = Long.parseLong(text.substring(pos1 + 1, pos2));
            if (action != null)
                return action.accept(conn.getInputStream(), conn.getOutputStream());
        } catch (IOException e) {
            throw new FTPClientException(e.getMessage());
        } finally {
            recv(); // 最后再接收一次消息
        }
        return -1;
    }

    // 按行读取文件
    public long lines(String path, int start,
                      int lines, FTPLineHandle handle) throws FTPClientException {
        return open0(path, Mode.OPEN, 0, (in, out) -> {
            long length = 0;
            BufferedReader reader = null;
            ManagerClientMonitor monitor = ManagerClientMonitor.monitoring(this, connTimeout);
            try {
                reader = new BufferedReader(new InputStreamReader(in));
                // 传统操作，低于java 1.8版本适用
                String line = null;
                int counter = 0;
                boolean contiune = false;
                while((line = reader.readLine()) != null) {
                    // 记录最后连接时间
                    recordLastConnectionTime();
                    length += line.getBytes().length + 1;
                    counter++;
                    contiune = (lines <= 0 || (lines > 0 && (counter - start) < lines));
                    // 如果判断出不能再读取则退出循环
                    if (!contiune)
                        break;
                    if (counter > start && contiune && handle != null) {
                        handle.apply(counter, line);
                    }
                }
            } catch (IOException e) {
                throw new FTPClientException(e.getMessage());
            } finally {
                monitor.stop();
            }
            return length;
        });
    }

    // 获取文件列表
    private List<FTPFile> listFile0(String path, Predicate<FTPFile> filter) throws FTPClientException {
        ArrayList<FTPFile> files = new ArrayList<>();
        Socket conn = channel();
        FTPResponse resp;
        if (path == null)
            resp = send("LIST");
        else
            resp = send("LIST", path);
        if (!resp.getStatus()) {
            return files;
        }
        BufferedReader reader = null;
        ManagerClientMonitor monitor = ManagerClientMonitor.monitoring(this, connTimeout);
        try {
            String line;
            reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            while((line = reader.readLine()) != null) {
                // 记录最后连接时间
                recordLastConnectionTime();
                boolean add = true;
                FTPFile file = FTPFile.parsex(path, line);
                if (filter != null)
                    add = filter.test(file);
                if (add)
                    files.add(FTPFile.parsex(path, line));
            }
        } catch (IOException e) {
            //e.printStackTrace();
            throw new FTPClientException(e.getMessage());
        } finally {
            try {
                if (conn != null)
                    conn.close();
            } catch (IOException e) {
                //e.printStackTrace();
            } finally {
                recv(); // 最后再接收一次消息
                monitor.stop();
            }
        }
        return files;
    }

    // 获取文件列表。需要指定路径和文件过滤器
    public List<FTPFile> listFile(String path, Predicate<FTPFile> filter) throws FTPClientException {
        return listFile0(path, filter);
    }

    // 获取文件列表。需要指定路径
    public List<FTPFile> listFile(String path) throws FTPClientException {
        return listFile0(path, null);
    }

    // 获取文件列表。需要指定文件过滤器，在不指定路径的情况下，使用的是 CWD 或 PWD 所对应的路径。
    public List<FTPFile> listFile(Predicate<FTPFile> filter) throws FTPClientException {
        return listFile0(null, filter);
    }

    // 获取文件列表。在不指定路径的情况下，使用的是 CWD 或 PWD 所对应的路径。
    public List<FTPFile> listFile() throws FTPClientException {
        return listFile0(null, null);
    }

    // 指定路径向下钻取文件
    private List<FTPFile> exploreFile0(String path, Predicate<FTPFile> filter,
                                       boolean containsDirectory) throws FTPClientException {
        List<FTPFile> files = new ArrayList<>();
        // 获取指定路径下所有文件信息，该步骤不进行过滤，以免不能检索到目录
        List<FTPFile> all = listFile0(path, null);
        if (containsDirectory) {
            all.stream().filter(FTPFile::isDirectory).forEach(files::add);
        }
        // 将文件类型的数据加入列表
        if (filter != null) {
            all.stream().filter(FTPFile::isFile).filter(filter).forEach(files::add);
        } else {
            all.stream().filter(FTPFile::isFile).forEach(files::add);
        }
        // 向下级目录钻取文件，直到没有下级目录为止
        all.stream().filter(FTPFile::isDirectory).forEach(dir -> {
            // 是否添加目录
            if (containsDirectory) files.add(dir);
            // 递归调用本方法
            // System.out.println("scan directory: " + dir.getFullPath());
            files.addAll(exploreFile0(dir.getFullPath(), filter, containsDirectory));
        });
        return files;
    }

    /**
     * 钻取文件列表
     * @param path 指定路径
     * @param filter 文件过滤
     * @param containsDirectory 列表中是否包含目录
     * @return 返回文件列表
     */
    public synchronized List<FTPFile> exportFiles(String path, Predicate<FTPFile> filter,
                                                  boolean containsDirectory) throws FTPClientException {
        return exploreFile0(path, filter, containsDirectory);
    }

    // 指定路径向下钻取目录，该操作不会存放文件
    private synchronized List<FTPFile> exploreDirectory0(String path) throws FTPClientException {
        List<FTPFile> directories = new ArrayList<>();
        // 将止录类型的数据加入列表
        directories.addAll(listFile0(path, FTPFile::isDirectory));
        directories.stream().filter(dir -> dir.getDirectoryCount() > 2).forEach(dir -> {
            directories.addAll(exploreDirectory0(dir.getFullPath()));
        });
        return directories;
    }

    // 取目录列表
    // 如果需要向下钻取，则可以设置 explore = true。
    public synchronized List<FTPFile> listDirectories(boolean explore) throws FTPClientException {
        if (explore) {
            List<FTPFile> directories = new ArrayList<>();
            listFile0(null, FTPFile::isDirectory).forEach(file -> {
                directories.add(file);
                directories.addAll(exploreDirectory0(file.getFullPath()));
            });
            return directories;
        } else {
            return listFile0(null, FTPFile::isDirectory);
        }
    }

    /**
     * 上传文件
     * @param local 将要上传的本地文件
     * @param path 指定上传的路径
     * @param append 是否追加数据
     * @param status 指定文件状态
     * @throws FTPClientException 上传过程中出错则抛出该异常。
     */
    public synchronized void put(File local, String path,
                                 boolean append, @NotNull final FileStatus status) throws FTPClientException {
        open0(path, (append ? Mode.APPEND : Mode.OVERWRITE), 0, (in, out) -> {
            status.setStartTime();
            status.setLocalFile(local);
            //ManagerClientMonitor monitor = ManagerClientMonitor.monitoring(this, connTimeout);
            try (FileInputStream fin = new FileInputStream(local)) {
                byte[] buffer = new byte[1024];
                while (true) {
                    int len = fin.read(buffer, 0, 1024);
                    if (len <= -1) break;
                    out.write(buffer, 0, len);
                    status.setLength(x -> x + len);
                    // 记录最后连接时间
                    recordLastConnectionTime();
                }
                out.flush();
                status.setStatus(FileStatus.STATUS_SENT);
                status.setEndTime();
            } catch (IOException e) {
                status.setStatus(FileStatus.STATUS_ABORT);
                throw new FTPClientException(e.getMessage());
            }
            return status.getLength();
        });
    }

    /**
     * 上传文件
     * @param local 将要上传的本地文件
     * @param path 指定上传的路径
     * @param append 是否追加数据
     * @return 返回文件状态。
     * @throws FTPClientException 上传过程中出错则抛出该异常。
     */
    public synchronized FileStatus put(File local, String path, boolean append) throws FTPClientException {
        FileStatus status = new FileStatusImpl(path);
        put(local, path, append, status);
        return status;
    }

    // 登陆。使用指定的用户和密码登陆 FTP 服务器。
    public synchronized boolean login(String user, String pass) throws FTPClientException {
        FTPResponse resp;
        // 发送用户名称
        resp = USER(user);
        if (!resp.getStatus())
            return false;
        // 使用密码
        resp = PASS(pass);
        return resp.getStatus();
    }

    // 登陆。此时使用的是 conf 中所配置的用户和密码。
    public synchronized boolean login() throws FTPClientException {
        return login(conf.getUser(), conf.getPass());
    }

    /**
     * 退出登陆。
     * 该操作会关闭FTP连接。
     */
    public synchronized void quit() throws FTPClientException {
        if (isConnected())
            send("QUIT");
    }

    // 设置当前客户端工作路径
    public synchronized boolean setCurrentWorkDirectory(String path) throws FTPClientException  {
        return CWD(path).getStatus();
    }


    // FTPResponse 接口实现类
    private final class FTPResopnseImpl implements FTPResponse {

        private int    code;
        private String text;

        public FTPResopnseImpl(int code, String text) {
            this.code = code;
            this.text = text;
        }

        @Override
        public int getCode() {
            return code;
        }

        @Override
        public String getText() {
            return text;
        }

        @Override
        public boolean getStatus() {
            return code > 0 && code < 400;
        }

        @Override
        public String toString() {
            return code + " " + text;
        }

    }

    // FileStatus 实现
    private final static class FileStatusImpl implements FileStatus {
        int status;
        long length;
        long lines;
        long startTime;
        long endTime;
        String name;
        File   local;

        private FileStatusImpl(String name) {
            this.name = name;
        }

        public long getLength() {
            return length;
        }

        public long getLines() {
            return lines;
        }

        public File getLocalFile() {
            return local;
        }

        public String getName() {
            return name;
        }

        public int getStatus() {
            return status;
        }

        public void setLength(long length) {
            this.length = length;
        }

        public void setLength(LongFunction<Long> action) {
            length = action.apply(length);
        }

        public void setLines(long lines) {
            this.lines = lines;
        }

        public void setLocalFile(File local) {
            this.local = local;
        }

        public void setEndTime(long time) {
            this.endTime = time;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setStartTime(long time) {
            this.startTime = time;
        }

        public void setStatus(int status) {
            this.status = status;
        }


    }
}