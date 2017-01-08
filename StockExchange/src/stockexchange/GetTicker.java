package stockexchange;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//Driver Class
public class GetTicker extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new GetTicker(), args);
		System.exit(res);

	}
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
      	Job job = Job.getInstance(conf);	
		job.setJarByClass(GetTicker.class);
		job.setJobName("Ticker Single");
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,StockMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class,StockMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class,StockMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[3]), TextInputFormat.class,StockMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[4]));
		
		//job.setMapperClass(StockMapper.class);
		job.setNumReduceTasks(1);
		
		job.setReducerClass(StockVariationReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	

}
