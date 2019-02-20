package com.liuyang.tools;

import com.sun.istack.internal.NotNull;

import java.util.List;

public final class JSON {


    public String toJSON(@NotNull List<?> list) {
        return "[" + StringUtils.join(',', true, list) + "]";
    }
}
