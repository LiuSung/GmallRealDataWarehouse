package com.gmall.realtime.utils;

public class MysqlUtil {
    public static String getBaseDicLookUpDDL(){
        return "create table `base_dic`(\n" +
                "`dic_code` string,\n" +
                "`dic_name` string,\n" +
                "`parent_code` string,\n" +
                "`create_time` timestamp,\n" +
                "`operate_time` timestamp,\n" +
                "primary key(`dic_code`) not enforced\n" +
                ")" + MysqlUtil.mysqlLookUpTableDDL("base_dic");
    }
    public static String mysqlLookUpTableDDL(String tableName){
        return "WITH (\n" +
                "'connector' = 'jdbc',\n" +
                "'url' = 'jdbc:mysql://192.168.141.100:3306/gmall',\n" +
                "'table-name' = '" + tableName + "',\n" +
                "'lookup.cache.max-rows' = '10',\n" +
                "'lookup.cache.ttl' = '1 hour',\n" +
                "'username' = 'root',\n" +
                "'password' = '000000',\n" +
                "'driver' = 'com.mysql.cj.jdbc.Driver'\n" +
                ")";
    }
}
