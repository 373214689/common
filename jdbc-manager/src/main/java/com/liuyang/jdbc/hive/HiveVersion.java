package com.liuyang.jdbc.hive;

/**
 * Hive Server Protocol Version
 *
 * @author liuyang
 * @version 1.0.0
 */
public enum HiveVersion {
    /** Hive Server Version 1 */
    VERSION_1("org.apache.hadoop.hive.jdbc.HiveDriver", "jdbc:hive"),
    /** Hive Server Version 2 */
    VERSION_2("org.apache.hive.jdbc.HiveDriver", "jdbc:hive2");

    private String driver;
    private String schema;

    // Construct Method.
    HiveVersion(String driver, String schema) {
        this.driver = driver;
        this.schema = schema;
    }

    /**
     * Get JDBC Driver Name
     * @return Return JDBC driver name.
     */
    public String getDriver() {
        return driver;
    }

    /**
     * Get JDBC Schema.
     * @return Return JDBC shema.
     */
    public String getSchema() {
        return schema;
    }
}
