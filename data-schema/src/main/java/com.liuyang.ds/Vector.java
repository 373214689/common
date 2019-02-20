package com.liuyang.ds;

public interface Vector<T> {

    T get(int index);

    void fill(T value);

    void reset();

    int size();
}
