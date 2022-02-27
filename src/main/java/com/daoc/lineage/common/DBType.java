package com.daoc.lineage.common;

public enum DBType {

    HSQL("HSQL","hsql"),
    DB2("DB2","db2"),
    PG("PG","postgresql"),
    SQLSERVER("SS","sqlserver"),
    SYBASE("SB","sybase"),
    ORACLE("ORACLE","oracle"),
    MYSQL("MYSQL","mysql"),
    MARIADB("MDB","mariadb"),
    HBASE("HBASE","hbase"),
    HIVE("HIVE","hive"),
    KINGBASE("KB","kingbase"),
    GBASE("GB","gbase"),
    ODPS("ODPS","odps"),
    OCEANBASE("OB","oceanbase"),
    TERADATA("TD","teradata"),
    PHOENIX("PH","phoenix"),
    CLICKHOUSE("CK","clickhouse");
    private String dbName;
    private String dbType;

    DBType(String dbType, String dbName){
        this.dbType = dbType;
        this.dbName = dbName;
    }

    public static String getDbName(String dbType){
        for (DBType value : DBType.values()) {
            if(value.dbType.equals(dbType)){
                return value.dbName;
            }
        }
        return null;
    }

}
