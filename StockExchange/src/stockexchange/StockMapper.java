package stockexchange;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.log4j.Logger;

public class StockMapper extends Mapper<LongWritable, Text, Text, Text>{
//	private Logger logger = Logger.getLogger(StockMapper.class);
	//private MultipleOutputs<NullWritable , Writable> mOutputs; 
	
	@Override
	public void map(LongWritable key, Text value,Context context)
	throws IOException , InterruptedException {
//		String line = value.toString();
//		String Ticker = line.substring(5, 8);
//		String Det = line.substring(9);
		String[] line = value.toString().split("\t+");
		String Ticker = line[1].trim();
		String Det = line[2].trim()+("\t+")+line[3].trim()+("\t+")+line[4].trim()+("\t+")+line[5].trim()+("\t+")+line[6].trim()+("\t+")+line[7].trim()+("\t+")+line[8].trim();

		context.write(new Text(Ticker), new Text(Det));
	}
	
	
}
