package com.iflytek.edu.hbase.test.PartitionRowKey;

import org.apache.hadoop.hbase.util.Bytes;

public class PartitionRowKeyManager implements RowKeyGenerator,
        SplitKeysCalculator {

    public static final int DEFAULT_PARTITION_AMOUNT = 20;
    private long currentId = 1;
    private int partition = DEFAULT_PARTITION_AMOUNT;
    public void setPartition(int partition) {
        this.partition = partition;
    }

    public byte[] nextId() {
        try {
            long partitionId = currentId % partition;
            return Bytes.add(Bytes.toBytes(partitionId),
                    Bytes.toBytes(currentId));
        } finally {
            currentId++;
        }
    }

    public byte[][] calcSplitKeys() {
        byte[][] splitKeys = new byte[partition - 1][];
        for(int i = 1; i < partition ; i ++) {
            splitKeys[i-1] = Bytes.toBytes((long)i);
        }

        /**
         * rowkey=0002rer4343343422,则当前这条数据就会保存到0001|~0002|这个region里，
         * 因为我的messageId都是字母+数字，“|”的ASCII值大于字母、数字。
         *
         */
        /**
         * splitkeys = [1,2,3,4]
                     * [, 1)*** 0xx, <1
                     * [1,2)*** 1xx, <2
                     * [2,3)*** 2xx, <3 <=== rowkey=0002rer4343343422
                     * [3,4)*** 3xx, <4
                     * [4, )*** 4xx
         *
         * splitkeys = [1|,2|,3|,4|]
         *          * [,1|)***  0xx, <1|xx
         *          * [1|,2|)*** >1|xx , <2|xx <=== rowkey=0002rer4343343422
         *          * [2|,3|)***
         *          * [3|,4|)***
         *          * [4|,)***
         *
         */
        return splitKeys;
    }
}
