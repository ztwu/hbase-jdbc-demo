package com.iflytek.edu.hbase.test;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class Test {
    @org.junit.Test
    public void testHashAndCreateTable() throws Exception{
        HashChoreWoker worker = new HashChoreWoker(1000000,10);
        byte [][] splitKeys = worker.calcSplitKeys();
        for(byte[] b:splitKeys){
            System.out.println(Bytes.toString(b));
            System.out.println(Bytes.toStringBinary(b));
        }

//        HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
//        TableName tableName = TableName.valueOf("hash_split_table");
//
//        if (admin.tableExists(tableName)) {
//            try {
//                admin.disableTable(tableName);
//            } catch (Exception e) {
//            }
//            admin.deleteTable(tableName);
//        }
//
//        HTableDescriptor tableDesc = new HTableDescriptor(tableName);
//        HColumnDescriptor columnDesc = new HColumnDescriptor(Bytes.toBytes("info"));
//        columnDesc.setMaxVersions(1);
//        tableDesc.addFamily(columnDesc);
//
//        admin.createTable(tableDesc ,splitKeys);
//
//        admin.close();
    }
}
