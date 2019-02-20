package com.liuyang.ds.vectors;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class IntVector  {

    private int[] col;

    private int size;
    private int mark;

    public IntVector() {
        col = new int[1024];
    }

    public IntVector(int intCapacity) {
        if (intCapacity >= Integer.MAX_VALUE || intCapacity < 0)
            throw new IllegalArgumentException("Illegal intCapacity parameter [" + intCapacity + "].");

        col = new int[1024];
    }

    private void rangeCheck(int index, int range) {
        if (index >= range || index < 0)
            throw new IndexOutOfBoundsException("index out of range [index = " + index + ", size: " + size + "].");
    }

    public void add(int value) {
        if (mark + 1 > size)
            throw new IndexOutOfBoundsException("index out of range [index = " + mark + ", size: " + size + "].");
        col[mark++] = value;
    }

    public int get(int index) {
        return 0;
    }

    public void fill(int value) {
        for (int i = mark; i < size; i++)
            col[i] = value;
    }


    public void reset() {
        mark = 0;
    }

    public int set(int index, int value) {
        rangeCheck(index, mark);
        int old = col[index];
        col[index] = value;
        return old;
    }

    public int size() {
        return mark;
    }

    public void read(InputStream in) throws IOException {

    }

    public void write(OutputStream out) throws IOException {

    }
}
