package com.liuyang.hadoop.data;

import java.util.NoSuchElementException;

public enum FileType {
    ORC("ORC", ".orc"),
    PARQUET("PARQUET", "parquet"),
    TEXTFILE("TEXTFILE", ".txt");

    private String name;
    private String suffix;
    private FileType(String name, String suffix) {
        this.name   = name;
        this.suffix = suffix;
    }

    public String getName() {
        return name;
    }

    public String getSuffix() {
        return suffix;
    }

    public static FileType find(String name) {
        for (FileType st : values()) {
            if (st.name.equals(name))
                return st;
        }
        throw new NoSuchElementException("Not matched by " + name);
    }
}
