package com.iflytek.edu.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2018/3/13
 * Time: 11:56
 * Description
 */

public class HbaseJdbcTest {

    public static void main(String[] args) throws Exception {

//        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "192.168.1.100,192.168.1.101");

        // 创建表
        /*String[] family = {"user_id","user_name"};
        createTable("ztwu", family, conf);*/

        // 为表添加数据
        /*String[] column1 = { "id", "no" };
        String[] value1 = {"1","001"};
        String[] column2 = { "name", "login_name" };
        String[] value2 = { "ztwu", "ztwu2" };
        addData("s01", "ztwu", column1, value1, column2, value2, conf);
        addData("s02", "ztwu", column1, value1, column2, value2, conf);
        addData("s03", "ztwu", column1, value1, column2, value2, conf);*/

        //查询 ===》get
//        getResult("ztwu", "s02", conf);
        //查询某一列
//        getResultByColumn("ztwu","s02","user_id","no",conf);
        //查询某数据的多个版本
//        getResultByVersion("ztwu","s01","age","num",conf);

        //遍历hbase表 ===》scan
//        getResultScann("ztwu", conf);
        //按rowkey范围遍历hbase表
        //[start_rowkey,stop_rowkey)
//        getResultScann("ztwu","s01","s03",conf);


        // 更新列
        /*updateTable("ztwu", "s02", "user_id",
                "id", "6", conf);*/

        //删除列
//        deleteColumn("ztwu2","r0","c0","id", conf);

        byte[] temp0 = Bytes.toBytes("r10");
        byte[] temp1 = Bytes.toBytes("r11");
        byte[] temp2 = Bytes.toBytes(12);
        System.out.println(Arrays.toString(temp0));
        System.out.println(Arrays.toString(temp1));
        System.out.println(Arrays.toString(temp2));

    }

    /**
     * hbase新建表
     * @param tableName
     * @param family
     * @param conf
     * @throws Exception
     */
    public static void createTable(String tableName, String[] family, Configuration conf) throws Exception {

        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor desc = new HTableDescriptor(tableName);
        for (int i = 0; i < family.length; i++) {
            //HColumnDescriptor添加列族
            desc.addFamily(new HColumnDescriptor(family[i]));
        }
        if (admin.tableExists(tableName)) {
            System.out.println("table Exists!");
            System.exit(0);
        } else {
            admin.createTable(desc);
            System.out.println("create table Success!");
        }

    }

    /**
     * 删除表
     *
     * @tableName 表名
     */
    public static void deleteTable(String tableName, Configuration conf) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        System.out.println(tableName + "is deleted!");
    }

    /**
     * 为表添加数据（适合知道有多少列族的固定表）
     *
     * @rowKey rowKey
     *
     * @tableName 表名
     *
     * @column1 第一个列族列表
     *
     * @value1 第一个列的值的列表
     *
     * @column2 第二个列族列表
     *
     * @value2 第二个列的值的列表
     */
    public static void addData(String rowKey, String tableName,
                               String[] column1, String[] value1, String[] column2, String[] value2, Configuration conf)
            throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
        HTable table = new HTable(conf, Bytes.toBytes(tableName));// HTabel负责跟记录相关的操作如增删改查等//
        // 获取表
        HColumnDescriptor[] columnFamilies = table.getTableDescriptor() // 获取所有的列族
                .getColumnFamilies();

        for (int i = 0; i < columnFamilies.length; i++) {
            String familyName = columnFamilies[i].getNameAsString(); // 获取列族名
            if (familyName.equals("user_id")) { // user_id列族put数据
                for (int j = 0; j < column1.length; j++) {
                    put.add(Bytes.toBytes(familyName),
                            Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
                }
            }
            if (familyName.equals("user_name")) { // user_name列族put数据
                for (int j = 0; j < column2.length; j++) {
                    put.add(Bytes.toBytes(familyName),
                            Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));
                }
            }
        }
        table.put(put);
        System.out.println("add data Success!");
    }

    /**
     * 根据rwokey查询
     *
     * @rowKey rowKey
     *
     * @tableName 表名
     */
    public static Result getResult(String tableName, String rowKey, Configuration conf)
            throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        HTable table = new HTable(conf, Bytes.toBytes(tableName));// 获取表
        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            System.out.println("family:" + Bytes.toString(kv.getFamily()));
            System.out .println("qualifier:" + Bytes.toString(kv.getQualifier()));
            System.out.println("value:" + Bytes.toString(kv.getValue()));
            System.out.println("Timestamp:" + kv.getTimestamp());
            System.out.println("-------------------------------------------");
        }
        return result;
    }

    /**
     * 遍历查询hbase表
     *
     * @tableName 表名
     */
    public static void getResultScann(String tableName, Configuration conf) throws IOException {
        Scan scan = new Scan();
        ResultScanner rs = null;
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (KeyValue kv : r.list()) {
                    System.out.println("row:" + Bytes.toString(kv.getRow()));
                    System.out.println("family:" + Bytes.toString(kv.getFamily()));
                    System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
                    System.out.println("value:" + Bytes.toString(kv.getValue()));
                    System.out.println("timestamp:" + kv.getTimestamp());
                    System.out.println("-------------------------------------------");
                }
            }
        } finally {
            rs.close();
        }
    }

    /**
     * 遍历查询hbase表
     *
     * @tableName 表名
     */
    public static void getResultScann(String tableName, String start_rowkey,
                                      String stop_rowkey, Configuration conf) throws IOException {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(start_rowkey));
        scan.setStopRow(Bytes.toBytes(stop_rowkey));
        ResultScanner rs = null;
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (KeyValue kv : r.list()) {
                    System.out.println("row:" + Bytes.toString(kv.getRow()));
                    System.out.println("family:" + Bytes.toString(kv.getFamily()));
                    System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
                    System.out.println("value:" + Bytes.toString(kv.getValue()));
                    System.out.println("timestamp:" + kv.getTimestamp());
                    System.out.println("-------------------------------------------");
                }
            }
        } finally {
            rs.close();
        }
    }

    /**
    * 查询表中的某一列
    *
    * @tableName 表名
    *
    * @rowKey rowKey
    */
    public static void getResultByColumn(String tableName, String rowKey,
                                         String familyName, String columnName, Configuration conf) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName)); // 获取指定列族和列修饰符对应的列
        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            System.out.println("family:" + Bytes.toString(kv.getFamily()));
            System.out
                    .println("qualifier:" + Bytes.toString(kv.getQualifier()));
            System.out.println("value:" + Bytes.toString(kv.getValue()));
            System.out.println("Timestamp:" + kv.getTimestamp());
            System.out.println("-------------------------------------------");
        }
    }


    /**
     * 查询某列数据的多个版本
     *
     * @tableName 表名
     *
     * @rowKey rowKey
     *
     * @familyName 列族名
     *
     * @columnName 列名
     */
    public static void getResultByVersion(String tableName, String rowKey,
                                          String familyName, String columnName, Configuration conf) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
        get.setMaxVersions(3);
        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            System.out.println("family:" + Bytes.toString(kv.getFamily()));
            System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
            System.out.println("value:" + Bytes.toString(kv.getValue()));
            System.out.println("Timestamp:" + kv.getTimestamp());
            System.out.println("-------------------------------------------");
        }
    }


    /**
     * 更新表中的某一列
     *
     * @tableName 表名
     *
     * @rowKey rowKey
     *
     * @familyName 列族名
     *
     * @columnName 列名
     *
     * @value 更新后的值
     */
    public static void updateTable(String tableName, String rowKey,
                                   String familyName, String columnName, String value, Configuration conf)
            throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName),
                Bytes.toBytes(value));
        table.put(put);
        System.out.println("update table Success!");
    }

    /**
     * 删除指定的列
     *
     * @tableName 表名
     *
     * @rowKey rowKey
     *
     * @familyName 列族名
     *
     * @columnName 列名
     */
    public static void deleteColumn(String tableName, String rowKey,
                                    String falilyName, String columnName, Configuration conf) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
        deleteColumn.deleteColumns(Bytes.toBytes(falilyName),
                Bytes.toBytes(columnName));
        table.delete(deleteColumn);
        System.out.println(falilyName + ":" + columnName + "is deleted!");
    }

}
