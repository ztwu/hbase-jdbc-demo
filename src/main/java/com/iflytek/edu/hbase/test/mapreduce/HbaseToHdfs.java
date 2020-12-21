package com.iflytek.edu.hbase.test.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HbaseToHdfs {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Configuration conf=HBaseConfiguration.create();
        conf.set("fs.defaultFS", "hdfs://bd1906/");//mapreduce相关的，设置为高可用
        conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");//指定zookeeper
        //Connection connection=ConnectionFactory.createConnection(conf);
        //启动一个工作
        Job job=Job.getInstance(conf);
        //指定工作的入口
//        job.setJarByClass(HbaseToHdfs.class);
        //指定reduce的入口
        job.setReducerClass(MyReduce.class);
        //指定map的入口
        job.setMapperClass(MyMapper.class);
        //指定map输出的key,value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //指定reduce输出的key,value
        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(IntWritable.class);
        //指定hbase的表
        Scan scan=new Scan();
        //scan.addColumn(family, qualifier);
        TableMapReduceUtil.initTableMapperJob("HbaseAPI:HbaseAPI01", scan, MyMapper.class, Text.class, IntWritable.class, job,false);
        //指定输出
        FileSystem fs=FileSystem.get(conf);
        Path output=new Path("/user/hbase/data/hbasetohdfs");
        if(fs.exists(output)){
            fs.delete(output, true);
        }
        //FileOutputFormat指定工作的输出路径
        FileOutputFormat.setOutputPath(job, output);
        //提交工作
        job.waitForCompletion(true);

    }
    /**
     * 泛型1：输出的key的类型
     * 泛型2：输出的value的类型
     *
     */
    static class MyMapper extends TableMapper<Text, IntWritable>{
        /**
         * 参数1：行键对象
         * 参数2：行键那一行的结果集
         */
        //创建输入输出对象
        Text mk=new Text();
        IntWritable mv=new IntWritable();
        @Override
        protected void map(ImmutableBytesWritable key, Result value,
                           Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            //listCells()方法取出所有的单元格
            List<Cell> Cells = value.listCells();
            //遍历单元格，做k,v处理
            for (Cell cell : Cells) {
                String qualifier=Bytes.toString(CellUtil.cloneQualifier(cell));
                if (qualifier.equals("age")) {
                    String k=Bytes.toString(CellUtil.cloneValue(cell));
                    mk.set(k);
                    mv.set(1);
                    context.write(mk, mv);
                }
            }
        }
    }
    /**
     * 参数1：输入key的类型
     * 参数2：输入value的类型
     * 参数2：输出key的类型
     * 参数4：输出value的类型
     */
    static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
        IntWritable mv=new IntWritable();
        @Override
        /**
         * 参数1：输入的key
         * 参数2：map端的value集合
         * key相同的启动一个reducetask,对values处理，v累=累加
         */
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int count=0;
            for (IntWritable v : values) {
                count+=v.get();
            }
            mv.set(count);
            context.write(key, mv);
        }

    }
}
