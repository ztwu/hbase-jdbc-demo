package com.iflytek.edu.hbase.test.PartitionRowKey;

import com.iflytek.edu.hbase.test.PartitionRowKey.HashChoreWoker;
import org.apache.hadoop.hbase.util.Bytes;

public class Test {
    @org.junit.Test
    public void testHashAndCreateTable() throws Exception{
        HashChoreWoker worker = new HashChoreWoker(1000000,10);
        byte [][] splitKeys = worker.calcSplitKeys();
        for(byte[] b:splitKeys){
            System.out.println(Bytes.toString(b));
        }

        PartitionRowKeyManager partitionRowKeyManager = new PartitionRowKeyManager();
        byte [][] splitKeys2 = partitionRowKeyManager.calcSplitKeys();
        for(byte[] b:splitKeys2){
            System.out.println(Bytes.toString(b));
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
