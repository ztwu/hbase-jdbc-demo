package com.iflytek.edu.hbase.test.batch;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 需求：用BulkLoad的方法导入数据
 * @author Setsuna
 * @DataFormat 1       info:www.baidu.com      BaiDu
 */
public class GenerateHFile extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{

    @Override
    protected void map(LongWritable Key, Text Value,
                       Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
            throws IOException, InterruptedException {

        //切分导入的数据
        String Values=Value.toString();
        String[] Lines=Values.split("\t");
        String Rowkey=Lines[0];
        String ColumnFamily=Lines[1].split(":")[0];
        String Qualifier=Lines[1].split(":")[1];
        String ColValue=Lines[2];

        //拼装rowkey和put
        ImmutableBytesWritable PutRowkey=new ImmutableBytesWritable(Rowkey.getBytes());
        Put put=new Put(Rowkey.getBytes());
        put.addColumn(ColumnFamily.getBytes(), Qualifier.getBytes(), ColValue.getBytes());

        context.write(PutRowkey,put);
    }

}
