import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
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
import org.apache.hadoop.fs.FileSystem; 
import org.apache.hadoop.fs.Path; 

public class FileList {

	public static void main(String[] args) throws Exception { 
	    Configuration conf = new Configuration(); 
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    FileSystem fs = FileSystem.get(conf); 
        Path inputPaths[] = FileInputFormat.getInputPaths(conf); 
        for (int i = 0; i < inputPaths.length; i++) { 
          if (!fs.exists(inputPaths[i])) { 
            try { 
              fs.mkdirs(inputPaths[i]); 
            } catch (Exception e) { 
 
            }    
	}
	
}
