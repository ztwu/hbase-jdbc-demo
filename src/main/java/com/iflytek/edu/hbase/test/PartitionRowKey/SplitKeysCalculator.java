package com.iflytek.edu.hbase.test.PartitionRowKey;

public interface SplitKeysCalculator {
    public byte[][] calcSplitKeys();
}
