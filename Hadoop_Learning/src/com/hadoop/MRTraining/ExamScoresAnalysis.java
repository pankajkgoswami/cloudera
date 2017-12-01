package com.hadoop.MRTraining;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ExamScoresAnalysis {

	
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text> {
		
		public void map(LongWritable key, Text value , Context context) throws IOException, InterruptedException{
		
			String line = value.toString();
			String data[] = line.split(",");
			
			context.write(new Text(data[2]),new Text(data[3]+",1"));
			
		}
	}
		
	public static class Reduce extends Reducer<Text,Text,Text,DoubleWritable>{
		
		public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException{
		Iterator<Text> it = values.iterator();
		
		Double totalScore=0d;
		Long candidates = 0l;
		while (it.hasNext()){
			String value = it.next().toString();
			String[] svalues = value.split(",");
			totalScore += Double.valueOf(svalues[0]);
			candidates += Long.valueOf(svalues[1]);
		}
		double Average = totalScore/candidates;
		context.write(key,new DoubleWritable(Average));
		}
	}
		
		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			// TODO Auto-generated method stub
			
			Path in = new Path(args[0]);
			Path out = new Path(args[1]);	
			
			
			Configuration conf = new Configuration();
			
			cleanOutput(conf,out);
			
			Job job = Job.getInstance(conf);
			job.setJarByClass(ExamScoresAnalysis.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setMapperClass(MapClass.class);
			job.setReducerClass(Reduce.class);
			
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			FileInputFormat.setInputPaths(job, in);
			FileOutputFormat.setOutputPath(job, out);
			
			job.submit();

		}	
		
		public static void cleanOutput(Configuration conf, Path path) throws IOException{
			FileSystem hdfs = FileSystem.get(conf);
			hdfs.delete(path,true);
			
		}
	}


