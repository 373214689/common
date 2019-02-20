package com.liuyang.ftp;

import com.sun.istack.internal.NotNull;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * FTP文件
 * @author liuyang
 *
 */
public class FTPFile {
    private final static String[] months_en = {
            "Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"};
    private final static String[] months_cn = {
            "1月","2月","3月","4月","5月","6月","7月","8月","9月","10月","11月","12月"};

    // Character
    private static PathType parsePathType(char c) {
        switch (c) {
            case '-': return PathType.FILE;
            case 'd': return PathType.DIRECTORY;
            case 'l': return PathType.LINK_SYMBOLIC;
            case 'b': return PathType.BLOCK_DEVICE;
            case 'c': return PathType.CHAR_DEVICE;
        }
        return PathType.UNKNOWN;
    }

    // 检索月份
    private static int getMonth(String month) {
        int index = Arrays.binarySearch(months_en, 0, months_en.length, month);
        if (index == -1)
            index = Arrays.binarySearch(months_cn, 0, months_en.length, month);
        return index;
    }

    // 解析时间， Aug 18  2016， Jan   2 16:13
    private static Date parseDate(String data) {
        String[] words = data.split(" ");
        int year; int month , day, hour = 0, minute = 0;
        // 创建日历板
        Calendar calendar = Calendar.getInstance();
        TimeZone timeZone = calendar.getTimeZone();
        timeZone.setRawOffset(0);
        // 检索月份
        month = getMonth(words[0]);
        // 检索日期
        day = Integer.parseInt(words[1]);
        calendar.set(Calendar.MONTH, month);
        calendar.set(Calendar.DAY_OF_MONTH, day);
        // 如果还有":"，表示时间，没有就表示年份
        if (words[2].indexOf(':') == -1) {
            year = Integer.parseInt(words[2]);
            calendar.set(Calendar.YEAR, year);
        } else {
            String[] time = words[2].split(":");
            hour = Integer.parseInt(time[0]);
            minute = Integer.parseInt(time[1]);
        }
        calendar.set(Calendar.HOUR_OF_DAY, hour);
        calendar.set(Calendar.MINUTE, minute);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.setTimeZone(timeZone);
        return calendar.getTime();
    }

    public static FTPFile parsex(@NotNull String parent, @NotNull String fileLink) {
        List<String> values = Stream.of(fileLink.split(" "))
                .filter(str -> !str.isEmpty()) // 清除多余的无效数据
                .collect(Collectors.toList());
        FTPFile file = new FTPFile();
        // 正确的 unix 格式数据有 9 列
        // 如：
        //     -rw-r--r--  1 root root  1 511855636 1月   8  22:42 xq700_201901080000.txt
        //     drwxr-x---  2 vftp vftp  0 12288     Jan   09 15:00 monitorbak
        if (values.size() == 9) {
            file.link       = fileLink;
            file.type       = parsePathType(values.get(0).charAt(0));
            file.permission = values.get(0).substring(1);
            file.dirs       = Long.parseLong(values.get(1));
            file.group      = values.get(2);
            file.owner      = values.get(3);
            file.size       = Long.parseLong(values.get(4));
            file.date       = parseDate(values.get(5) + " " + values.get(6) + " " + values.get(7));
            file.name       = values.get(8);
            file.parent     = (values.get(8).equals(parent)) ? "" : parent;
        } else if (values.size() == 1) {
            // 如果只有一行路径，则只取路径即可
            int pos         = values.get(0).lastIndexOf('/');
            if (pos > 0) {
                file.name       = values.get(0).substring(pos + 1);
                file.parent     = (values.get(0).substring(0, pos).equals(parent)) ? "" : parent;
            } else {
                file.name       = values.get(0);
                file.parent     = values.get(0).equals(parent) ? "" : parent;
            }

        }
        values.clear();
        return file;
    }

    private String   link       = null;
    private PathType type       = null;
    private String   permission = null;
    private long     dirs       = 0;
    private String   group      = null;
    private String   owner      = null;
    private long     size       = 0;
    private Date     date       = null;
    private String   name       = null;
    private String   parent;

    /** 额外属性 */
    private Map<String, Object> properties = new ConcurrentHashMap<>();

    private FTPFile () {

    }

    /**
     * 清除数据，回收内存。
     */
    @Override
    protected void finalize() {
        clear();
    }

    /**
     * 清除数据
     */
    public void clear() {
        properties.clear();
        name = null;
        size = 0;
        parent = null;
        type = null;
        group  = null;
        owner  = null;
        permission = null;
        date = null;
        link = null;
        properties = null;
    }

    public String getFullPath() {
        if (".".equals(name) || "..".equals(name)) {
            return String.format("%s%s", name, parent);
        } else {
            return String.format("%s/%s", "/".equals(parent) ? "" : parent, name);
        }
    }

    public String getName() {
        return name;
    }

    public String getParent() {
        return parent;
    }

    public long getDirectoryCount() {
        return dirs;
    }

    public long getSize() {
        return size;
    }

    public String getLink() {
        return link;
    }

    public Date getDate() {
        return date;
    }

    public PathType getType() {
        return type;
    }

    public Object setProperty(String name, Object value) {
        return properties.put(name, value);
    }

    public Object getProperty(String name) {
        return properties.get(name);
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public String getPropertyString(String name) {
        return String.valueOf(this.properties.get(name));
    }

    public int getPropertyInt(String name) {
        return Integer.parseInt(this.properties.get(name).toString());
    }

    public long getPropertyLong(String name) {
        return Long.parseLong(this.properties.get(name).toString());
    }

    public Set<String> getPropertyKeySet() {
        return this.properties.keySet();
    }

    public Collection<Object> getPropertyValues() {
        return this.properties.values();
    }

    public boolean isDirectory() {
        return this.type == PathType.DIRECTORY;
    }

    public boolean isFile() {
        return this.type == PathType.FILE;
    }

    // 删除属性
    public boolean removeProperty(String name) {
        return properties.remove(name) != null;
    }

    public void setPermission(String permission) {
        this.permission = permission;
    }

    public void setDirectoryCount(long count) {
        this.dirs = count;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public void setType(PathType type) {
        this.type = type;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public void setParent(String parent) {
        this.parent = parent;
    }

    public String toString() {
        String formatter =  "[type=%s, permission=%s, dirs=%s, group=%s, owner=%s, size=%s, date=%s, name=%s, " +
                "parent=%s, link=%s, properties=%s]";
        return String.format(formatter,
                type,
                permission,
                dirs,
                group,
                owner,
                size,
                date,
                name,
                parent,
                link,
                properties
        );
    }

}
