package com.hadoop.MRTraining;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

public class WordAnalysis {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		
		Configuration conf = new Configuration();
		
		cleanOutput(conf,out);
		Job job = Job.getInstance(conf);
		job.setJarByClass(WordAnalysis.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setMapperClass(MapClass.class);
		//job.setReducerClass(Reduce.class);
		job.setReducerClass(LongSumReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		
		job.submit();
		

	}

	public static class MapClass extends Mapper<LongWritable,Text,Text,LongWritable>	{
		public void map(LongWritable key, Text value , Context context) throws IOException, InterruptedException{
			
			String line = value.toString();
			
			String body = line.replaceAll("[^A-Za-z\\s]","").toLowerCase().replaceAll(" ", "");
			String[] characters  = body.split("");
			for(String character : characters)
			{
				if (character.contains("a")||character.contains("e")||character.contains("i")||character.contains("o")||character.contains("u") ){
					context.write(new Text(character),new LongWritable(1));
				}
			}
		
			
			
		}
	}
	
	public static void cleanOutput(Configuration conf, Path path) throws IOException{
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(path,true);
		
	}
	
	public static class Reduce extends Reducer<Text, LongWritable,Text,LongWritable> {
		
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
		Long sum=0l;
		Iterator<LongWritable> it = values.iterator();
		while(it.hasNext()){
			sum +=it.next().get();
		}
		context.write(key, new LongWritable(sum));
		}
	}
}
