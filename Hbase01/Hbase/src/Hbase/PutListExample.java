package Hbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
	public class PutListExample {
	public static void main(String[] args) throws IOException {
	Configuration conf = HBaseConfiguration.create();
	HTable table = new HTable(conf, "testtable");
	List<Put> puts = new ArrayList<Put>();
	
	Put put1 = new Put(Bytes.toBytes("row1"));
	put1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual3"),
	Bytes.toBytes("val04"));
	puts.add(put1);
	
	Put put2 = new Put(Bytes.toBytes("row1"));
	put2.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
	Bytes.toBytes("val09"));
	puts.add(put2);
	
	table.put(puts);
	//table.flushCommits();
	}
	}