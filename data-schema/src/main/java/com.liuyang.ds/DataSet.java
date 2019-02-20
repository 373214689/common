package com.liuyang.ds;

import java.util.stream.Stream;

public interface DataSet {
    //java.util.Collections;
    //java.util.stream.Collectors;
    //java.util.stream.Stream;

    void close();
    //public <R, A> R collect(Collector<? super Row, A, R> collector);

    Stream<Row> stream();

    DataSet take(long num);



}
