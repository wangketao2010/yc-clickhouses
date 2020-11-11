package com.yc.clickhouse.generator;

import java.util.Arrays;
import java.util.List;

/**
 * 功能描述
 *
 * @author wangkt
 * @date 2020/11/9
 */
public class Generate {
    /**
     * 定义clickHouse数据库连接参数
     */
    public static String URL = "jdbc:clickhouse://192.168.87.162:8123";
    public static String USERNAME = "";
    public static String PASSWORD = "";
    public static String DIREVER = "ru.yandex.clickhouse.ClickHouseDriver";


    public static void main(String[] args) {
        DBUtil.URL = URL;
        DBUtil.USERNAME = USERNAME;
        DBUtil.PASSWORD = PASSWORD;
        DBUtil.DIREVER = DIREVER;
        execute("com.yc.clickhouse", "", "default", Arrays.asList("t_test"));
    }
    /**
     * 实体类生成
     * @param domain    包路径
     * @param outputDir 输入到当前工程下文件夹
     * @param dbName    数据库名称
     * @param tableName 表名，可以定义多个表
     */
    public static void execute(String domain, String outputDir, String dbName, List<String> tableName) {
        new MetaUtil().execute(domain, outputDir, dbName, tableName);
    }

}
