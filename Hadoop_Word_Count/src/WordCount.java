import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
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
 
 
public class WordCount{
 
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(WordCount.class);
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
        
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //FileInputFormat.addInputPaths(job, "/root/data/wordcount/temp");
        
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        boolean status = job.waitForCompletion(true);
        
        if (status) {
            System.exit(0);
        } 
        else {
            System.exit(1);
        }
    }
 
}