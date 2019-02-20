package com.liuyang.ftp;

public enum PathType {
    /** 普通文件：. */
    FILE,
    /** 目录： d */
    DIRECTORY,
    /** 符号连接：l */
    LINK_SYMBOLIC,
    /** 块设备：b，如：内存条、磁盘、光盘区动器、仿真设备等等。*/
    BLOCK_DEVICE,
    /** 字符设备：c，如：CPU、网络适配器、显示器、打印机等等。*/
    CHAR_DEVICE,
    /** 未知类型 */
    UNKNOWN,
}
