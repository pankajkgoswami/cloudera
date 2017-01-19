package stockexchange;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class StockReducerMultiOutput extends Reducer<Text, Text, Text, Text> {
	private Text result = null;
	private MultipleOutputs<Text,Text> out;
	
	public void setup(Context context) {
		   out = new MultipleOutputs<Text,Text>(context);   
		 }
	
public void reduce(Text Key, Iterable<Text> values, Context context) throws IOException, InterruptedException
{
int count = 0;
   for(Text value : values)
   {
	   int mod = count%4;
	   result=value;
	   out.write(Key, result,Integer.toString(mod));
	 //  count++;   
	   
   }

   context.write(Key, new Text());
  
}
public void cleanup(Context context) throws IOException,InterruptedException {
    out.close();        
 }
}