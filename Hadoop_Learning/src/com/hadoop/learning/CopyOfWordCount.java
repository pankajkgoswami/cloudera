package com.hadoop.learning;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
 
public class CopyOfWordCount{
 
    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        // Set HADOOP user
        System.setProperty("HADOOP_USER_NAME", "cloudera");
        System.setProperty("hadoop.home.dir", "/");
		FileSystem fs = FileSystem.get(conf);
		conf.set("fs.defaultFS", "hdfs://quickstart.cloudera:8020");
		Path in_file = new Path("pkg/TVSELECT.csv");
		Path out_file = new Path("pkg/TVSELECT/");
        Job job = Job.getInstance(conf);
        job.setJarByClass(CopyOfWordCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setMapperClass(WordCountMapper.class);
        //job.setMapperClass(WrappedMapper.class);
      //  job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        // There is not IdentityREducer class in New MR API, instead use WrappedReducer
       //job.setReducerClass(WrappedReducer.class);
       // job.setNumReduceTasks(3);
        
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        
        FileInputFormat.setInputPaths(job, in_file);
        //FileInputFormat.addInputPaths(job, "/root/data/wordcount/temp");
        
        FileOutputFormat.setOutputPath(job, out_file);
        
        boolean status = job.waitForCompletion(true);
        
        if (status) {
            System.exit(0);
        } 
        else {
            System.exit(1);
        }
    }
 
}