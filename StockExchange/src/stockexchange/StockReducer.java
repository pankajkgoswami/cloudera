package stockexchange;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer; 

public class StockReducer extends Reducer<Text, Text, Text, Text> {
	private Text result = null;
	//private MultipleOutputs<Text,Text> out;
	
	//public void setup(Context context) {
	//	   out = new MultipleOutputs<Text,Text>(context);   
	//	 }
	
public void reduce(Text Key, Iterable<Text> values, Context context) throws IOException, InterruptedException
{
   for(Text value : values)
   {
	   result=value;
	   context.write(Key, result);
   }   
}
}