package stockexchange;

import java.io.IOException;



//import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer; 

public class StockVariationReducer extends Reducer<Text, Text, Text, Text> {
	
public void reduce(Text Key, Iterable<Text> values, Context context) throws IOException, InterruptedException
{
	double variation=0;
	double High;
	double low;
	String FinalDate=null;
	String result=null;
	
   for(Text value : values)
   {
	   String[] line = value.toString().split("\t+");
	   High = Double.parseDouble(line[1].replace("+", "")) ;
	   low = Double.parseDouble(line[2].replace("+", ""))  ;
	   String Date=line[0].trim();
	   if ((High-low)>variation)
	   {
	   	   variation=High-low;   
	   	   FinalDate = Date;
	   }
	   String var=Double.toString(variation);
   	   result= FinalDate + ("\t+")+ var;
	   
   }  
   context.write(Key, new Text(result));
}
}