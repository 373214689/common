package com.liuyang.tools;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public final class IOUtils {

    private static void closeInputStream(InputStream in) {
        if (in != null)
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    private static void closeOutputStream(OutputStream out) {
        if (out != null)
            try {
                out.flush();
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    /**
     * 流复制
     * <p>
     *     从输入流中复制数据到输出流。
     * </p>
     * @param in         输入流
     * @param out        输出流
     * @param bufferSize 缓冲区长度，建议取值：1024。
     * @param limitSize  复制长度限制。取值：小于或等于 0 表示复制所有数据，否则复制指定长度的数据。
     * @param closeable  可否关闭。取值：true 表示复制完成后关闭输入流和输出流, false 表示复制完成后不关闭。
     * @return 返回寮际复制数据的长度。如果返回 -1 则表示复制失败。
     */
    public static long copy(InputStream in, OutputStream out,
                            int bufferSize, long limitSize, boolean closeable) throws IOException {
        long total = 0, limit = limitSize;
        int  len, size;
        size = bufferSize <= 0 ? 4096 : bufferSize;
        size = size >= limit ? size : ((int) limit);
        byte[] buff = new byte[size];
        try {
            while((len = in.read(buff, 0, size)) != -1) {
                limit -= len; //
                int written = (limitSize > 0) ? (len - (int) Math.abs(limit)) : len;
                out.write(buff, 0, written);
                total += written;
                if (limitSize > 0 && limit <= 0) break;
            }
            return total;
        } finally {
            if (closeable) {
                closeInputStream(in);
                closeOutputStream(out);
            }
        }
    }
}
