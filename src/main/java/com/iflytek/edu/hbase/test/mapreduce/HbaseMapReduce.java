package com.iflytek.edu.hbase.test.mapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class HbaseMapReduce {
    public static String path1 = "hdfs://hadoop80:9000/FlowData.txt";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();  //也可以用HBaseConfiguration类
        conf.set("hbase.rootdir","hdfs://hadoop80:9000/hbase");
        conf.set("hbase.zookeeper.quorum","hadoop80");
        conf.set(TableOutputFormat.OUTPUT_TABLE,"wlan_log");//在这里需要指定表的名字：相当于输出文件的路径
        conf.set("dfs.socket.timeout","2000");

        Job job = new Job(conf,"HbaseApp");
//        job.setJarByClass(HbaseMapReduce.class);  //是否打jar包运行
        FileInputFormat.setInputPaths(job, new Path(path1));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(1);
        job.setPartitionerClass(HashPartitioner.class);

        job.setReducerClass(MyReducer.class);
//         job.setOutputKeyClass(Text.class);
//         job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TableOutputFormat.class);//不在是TextOutputFormat
//         FileOutputFormat.setOutputPath(job, new Path(path2));
        job.waitForCompletion(true);
    }

    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

        protected void map(LongWritable k1, Text v1,Context context)throws IOException, InterruptedException {
            String[] splited = v1.toString().split("\t");
            String reportTime = splited[0];
            String msisdn = splited[1];
            Date date = new Date(Long.parseLong(reportTime));
            String time = DateConvert.dateParse(date);
            String rowkey = msisdn+":"+time;//获取到行健
            context.write(new Text(rowkey),new Text(v1.toString())); //将行健rowkey和对应的记录进行输出
        }
    }

    public static class MyReducer extends TableReducer<Text, Text, NullWritable> {

        protected void reduce(Text k2, Iterable<Text> v2s,Context context)throws IOException, InterruptedException {
            for (Text v2 : v2s) {
                String[] splited = v2.toString().split("\t");
                /**添加记录的时候需要指定行健、列族、列名、数值***/
                Put put = new Put(k2.toString().getBytes());
                put.addColumn("cf".getBytes(),"reportTime".getBytes(), splited[0].getBytes());
                put.addColumn("cf".getBytes(),"msisdn".getBytes(), splited[1].getBytes());
                put.addColumn("cf".getBytes(),"apmac1".getBytes(), splited[2].getBytes());
                put.addColumn("cf".getBytes(),"apmac2".getBytes(), splited[3].getBytes());
                put.addColumn("cf".getBytes(),"host".getBytes(), splited[4].getBytes());
                put.addColumn("cf".getBytes(),"sitetype".getBytes(), splited[5].getBytes());
                put.addColumn("cf".getBytes(),"upPackNum".getBytes(), splited[6].getBytes());
                put.addColumn("cf".getBytes(),"downPackNum".getBytes(), splited[7].getBytes());
                put.addColumn("cf".getBytes(),"upPayLoad".getBytes(), splited[8].getBytes());
                put.addColumn("cf".getBytes(),"downPayLoad".getBytes(), splited[9].getBytes());
                put.addColumn("cf".getBytes(),"httpstatus".getBytes(), splited[10].getBytes());
                context.write(NullWritable.get(),put);
            }
        }
    }
}
class DateConvert
{
    public static String dateParse(Date  date)
    {
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddhhmmss");//构造一个日期解析器
        return df.format(date);
    }
}
