package com.liuyang.hadoop;

import com.liuyang.ds.Row;
import com.liuyang.ds.Schema;
import com.liuyang.ds.Type;
import com.liuyang.jdbc.Column;
import com.liuyang.jdbc.hive.HiveDataBase;
import com.liuyang.jdbc.hive.HiveTable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.FileMetadata;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestHDFS {

    private static HiveTable tmp_delay = new HiveTable(new HiveDataBase("tmp", "hive"), "delay");
    private static HiveTable tmp_speed = new HiveTable(new HiveDataBase("tmp", "hive"), "speed");
    private static HiveTable tmp_resp = new HiveTable(new HiveDataBase("tmp", "hive"), "resp");

    private static HDFSConfig conf = new HDFSConfig("master", 8022, "hive", "");


    static {
        try {
            System.load("D:\\usr\\local\\hadoop-3.0.3\\bin\\hadoop.dll");
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Native code library failed to load.\n" + e);
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        //getfile();
        badcell();
        //readLocalData();
        /*FileMetadata fm = null;
        //FileMetadata
        OrcFile.readerOptions(null).fileMetadata(null);
        try {
            Reader reader = OrcFile.createReader(null,  OrcFile.readerOptions(null).fileMetadata(fm));
            reader.getSchema();
            RecordReader records = reader.rows();

            records.nextBatch(reader.getSchema().createRowBatch());


        } catch (IOException e) {
            e.printStackTrace();
        }*/


    }

    private static void readLocalData() {
        try (HDFSManager hdfs = conf.getConnection().enableLocal()) {
            Path local = new Path("file:///home/xdr/test/hdfs.orc");
            Stream<Row> stream = null;
            stream = hdfs.readORC(local);
            if (stream != null)
                stream.forEach(System.out::println);
            Schema[] fields = new Schema[4];
            fields[0] = new Column("day", Type.INT);
            fields[1] = new Column("hour", Type.SMALLINT);
            fields[2] = new Column("eci", Type.BIGINT);
            fields[3] = new Column("value", Type.STRING);
            stream = hdfs.readText(new Path("file:///home/xdr/test/000054_0"), "\t", fields);
            if (stream != null) {
                hdfs.writeORC(new Path("file:///home/xdr/test/000054_0.orc"), true, stream);

            }
            if (hdfs.exists(new Path("file:///home/xdr/test/000054_0.orc"))) {
                System.out.println("successful");
                hdfs.readORC(new Path("file:///home/xdr/test/000054_0.orc")).forEach(System.out::println);
            } else  {
                System.out.println("failure");
            }
                //stream.forEach(System.out::println);


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void getfile() {
        try (HDFSManager hdfs = conf.getConnection()) {
            File local = new File("/home/xdr/hdfs.orc");

            Path remote = new Path("/user/hive/warehouse/cfg.db/city_info/000000_0");

            hdfs.get(local, remote, false);
        }
    }

    private static void badcell() {
        // 数据模型：响应时延差小区
        tmp_delay.addField(new Column("day", Type.INT));
        tmp_delay.addField(new Column("hour", Type.INT));
        tmp_delay.addField(new Column("eci", Type.LONG));
        tmp_delay.addField(new Column("delay", Type.DOUBLE));
        tmp_delay.setLocation("/data/response");
        tmp_delay.setDelimiter("\t");
        // 数据模型：速率差小区
        tmp_speed.addField(new Column("day", Type.INT));
        tmp_speed.addField(new Column("hour", Type.INT));
        tmp_speed.addField(new Column("eci", Type.LONG));
        tmp_speed.addField(new Column("speed", Type.DOUBLE));
        tmp_speed.setLocation("/data/speed");
        tmp_speed.setDelimiter("\t");
        // 数据模型：成功率差小区
        tmp_resp.addField(new Column("day", Type.INT));
        tmp_resp.addField(new Column("hour", Type.INT));
        tmp_resp.addField(new Column("eci", Type.LONG));
        tmp_resp.addField(new Column("rate", Type.DOUBLE));
        tmp_resp.setLocation("/data/succ_rate");
        tmp_resp.setDelimiter("\t");

        System.out.println("---------------------------------");
        // 统计时延质差
        getBadCell(tmp_delay, 17923).forEach((a, b) -> {
            System.out.println(a + " count: " + b);
        });
        System.out.println("---------------------------------");
        // 统计速率质差
        getBadCell(tmp_speed, 17923).forEach((a, b) -> {
            System.out.println(a + " count: " + b);
        });
        System.out.println("---------------------------------");
        // 统计响应质差
        getBadCell(tmp_resp, 17923).forEach((a, b) -> {
            System.out.println(a + " count: " + b);
        });
    }

    private static Map<Long, Integer> getBadCell (HiveTable table, int day) {
        // 初始化结果集
        Map<Long, Integer> result = new HashMap<>();
        // 使用数据模型，指向 HDFS 上储存路径。
        HiveTable tmp = table.clone().setName(x -> x + "_" + day);
        tmp.setLocation(table.getLocation() + "_" + day);
        // 连接到 HDFS
        try (HDFSManager hdfs = conf.getConnection()) {
            // Map 阶段：统计小区。groupby eci
            Map<Long, List<LteCellCounter>> map = hdfs.readDataFromHiveTable(tmp)
                    .map(LteCellCounter::parse)
                    .collect(Collectors.groupingBy(LteCellCounter::getId));
            // Reduce 阶段：统计质差小区。
            map.forEach((a, b) -> {
                if (b.size() >= 8) {
                    result.put(a, b.size());
                    // System.out.println(a + " count:"  + b.size() + " (disperse)");
                } else {
                    // 排序时间
                    Integer[] hours = b.stream().map(LteCellCounter::getHour).sorted().toArray(Integer[]::new);
                    // 连续时间检查算法：连续时间数为 4
                    int cnt = 0, length = hours.length, hour = 0, limit = 4;
                    boolean abort = false;
                    for (int i = 0; i < length; i++) {
                        if (i == 0)
                            hour = hours[i];
                        // 统计连续时间出现次数，如果其中
                        if (hours[i] - hour <= 1) {
                            // 没有中止时，进行计数。中止后，不再计数。
                            cnt += !abort ? 1 : 0;
                        } else if (hours[i] - hour > 1) {
                            // 统计到足够的数量后，如果出现不连续时，则中止计数
                            if (cnt > limit)
                                abort = true;
                            cnt = abort ? cnt : 0;
                        }
                        hour = hours[i];
                    }
                    if (cnt >= limit)
                        result.put(a, cnt);
                    //System.out.println(a + " count:"  + cnt + " (continuous)");
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        } catch (HDFSException e) {
            e.printStackTrace();
        }
        return result;
    }

    private static class LteCellCounter {
        private static LteCellCounter parse(Row row) {
            LteCellCounter counter = new LteCellCounter();
            counter.eci = row.getLong("eci");
            counter.hour = row.getInteger("hour");
            return counter;
        }

        long eci;
        int hour;
        LteCellCounter() {

        }

        long getId() {
            return eci;
        }

        int getHour() {
            return hour;
        }
    }
}
