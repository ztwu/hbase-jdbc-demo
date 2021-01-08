package com.iflytek.edu.hbase.test.PartitionRowKey;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.Iterator;
import java.util.TreeSet;

public class PartitionRowKeyManager2 {

    public static void main(String[] args) {
        String[] keys = new String[] { "10|", "20|", "30|", "40|", "50|",
                "60|", "70|", "80|", "90|" };
        byte[][] splitKeys = new byte[keys.length][];
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);//升序排序
        for (int i = 0; i < keys.length; i++) {
            rows.add(Bytes.toBytes(keys[i]));
        }
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int i=0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            splitKeys[i] = tempRow;
            i++;
        }

        for(byte[] b:splitKeys){
            System.out.println(Bytes.toString(b));
        };
    }
}
