package com.hadoop.learning;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReadFromHDFS {

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		conf.set("fs.defaultFS", "hdfs://quickstart.cloudera:8020");
		System.out.println(fs.getUri());
		Path file = new Path("demo.txt");
		if (fs.exists(file)) {
			System.out.println("File exists.");
		} else {
			// Writing to file
			FSDataOutputStream outStream = fs.create(file);
			outStream.writeUTF("Welcome to HDFS Java API!!!");
			outStream.close();
		}
		// Reading from file
		FSDataInputStream inStream = fs.open(file);
		String data = inStream.readUTF();
		System.out.println(data);
		inStream.close();
		fs.close();
	}
}
