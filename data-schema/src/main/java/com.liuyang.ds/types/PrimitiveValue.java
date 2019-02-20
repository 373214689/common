package com.liuyang.ds.types;

import com.liuyang.ds.Type;
import com.liuyang.ds.Value;
import com.sun.istack.internal.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

public abstract class PrimitiveValue implements Value, Serializable {

    protected transient Type type;

    /**
     * 构造方法，需要指定type
     * @param type
     */
    protected PrimitiveValue(@NotNull Type type) {
        this.type = type;
    }

    public Type getType() {
        return type;
    }


    /**
     * 获取数据在内存中的长度（该方法暂未实现）.
     * the array and string data length will changed with content.
     * <li>byte    : 1</li>
     * <li>boolean : 1</li>
     * <li>short   : 2</li>
     * <li>int     : 4</li>
     * <li>float   : 4</li>
     * <li>long    : 8</li>
     * <li>double  : 8</li>
     * <li>string  : the length of string bytes + 8</li>
     * @return The length of data in memery.
     */
    public int length() {
        throw new UnsupportedOperationException();
    }

    /**
     * 重置
     */
    public void reset() {
        throw new UnsupportedOperationException();
    }

    public abstract void setValue(PrimitiveValue value);

    /**
     * Write data to the specified output stream.(该功能待完善)
     * @param out the specified outputstream.
     * @throws IOException
     */
    abstract public void writeValue(OutputStream out) throws IOException;
    //abstract public void writeValue(RecordConsumer recordConsumer);

    /**
     * Read data from the specified input stream.(该功能待完善)
     * @param in the specified input stream
     * @throws IOException
     */
    abstract public void readValue(InputStream in) throws IOException;
}
