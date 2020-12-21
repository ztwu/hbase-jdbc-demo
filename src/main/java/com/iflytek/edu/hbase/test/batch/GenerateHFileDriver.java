package com.iflytek.edu.hbase.test.batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GenerateHFileDriver {

    public static void main(String[] args) throws Exception {

        /**
         * 获取Hbase配置，创建连接到目标表，表在Shell中已经创建好，建表语句create 'BulkLoad','Info'，这里注意HBase对大小写很敏感
         */
        Configuration conf=HBaseConfiguration.create();
        Connection conn=ConnectionFactory.createConnection(conf);
        Table table=conn.getTable(TableName.valueOf("BulkLoad"));
        Admin admin=conn.getAdmin();

        final String InputFile="hdfs://centos:9000/HBaseTest/input";
        final String OutputFile="hdfs://centos:9000/HBaseTest/output";
        final Path OutputPath=new Path(OutputFile);

        //设置相关类名
        Job job=Job.getInstance(conf,"BulkLoad");
        job.setJarByClass(GenerateHFileDriver.class);
        job.setMapperClass(GenerateHFile.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        //设置文件的输入路径和输出路径
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);
        FileInputFormat.setInputPaths(job, InputFile);
        FileOutputFormat.setOutputPath(job, OutputPath);

        //配置MapReduce作业，以执行增量加载到给定表中。
        HFileOutputFormat2.configureIncrementalLoad(job, table, conn.getRegionLocator(TableName.valueOf("BulkLoad")));

        //MapReduce作业完成，告知RegionServers在哪里找到这些文件,将文件加载到HBase中
        if(job.waitForCompletion(true)) {
            LoadIncrementalHFiles Loader=new LoadIncrementalHFiles(conf);
            Loader.doBulkLoad(OutputPath, admin, table, conn.getRegionLocator(TableName.valueOf("BulkLoad")));
        }
    }
}
