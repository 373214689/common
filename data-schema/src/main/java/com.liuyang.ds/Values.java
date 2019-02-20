package com.liuyang.ds;

import java.util.Map;

/**
 * Values Interface
 *
 * @author liuyang
 * @version 1.0.0
 */
public interface Values {

    /**
     * Get values as an array of Object.
     * @return Return an array of Object.
     */
    Object [] values();

    /**
     * Get map.
     * @return Return a map.
     */
    Map<String, Object> toMap();
}
