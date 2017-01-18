package Hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class GetExampleList {
	public static void main(String[] args) throws IOException {	
	
		byte[] cf1 = Bytes.toBytes("colfam1");
		byte[] qf1 = Bytes.toBytes("qual1");
		byte[] qf2 = Bytes.toBytes("qual2");
		byte[] row1 = Bytes.toBytes("row1");
		byte[] row2 = Bytes.toBytes("row2");
		
	Configuration conf = HBaseConfiguration.create();
	@SuppressWarnings("deprecation")
	HTable table = new HTable(conf, "testtable");
	List<Get> gets = new ArrayList<Get>();
	
	Get get1 = new Get(row1);
	get1.addColumn(cf1, qf1);
	gets.add(get1);
	Get get2 = new Get(row2);
	get2.addColumn(cf1, qf1);
	gets.add(get2);
	Get get3 = new Get(row2);
	get3.addColumn(cf1, qf2);
	gets.add(get3);
	Result[] results = table.get(gets);
	System.out.println("First iteration...");
	for (Result result : results) {
	String row = Bytes.toString(result.getRow());
	System.out.print("Row: " + row + " ");
	byte[] val = null;
	if (result.containsColumn(cf1, qf1)) {
	val = result.getValue(cf1, qf1);
	System.out.println("Value: " + Bytes.toString(val));
	}
	if (result.containsColumn(cf1, qf2)) {
	val = result.getValue(cf1, qf2);
	System.out.println("Value: " + Bytes.toString(val));
	}
	}
	System.out.println("Second iteration...");
	for (Result result : results) {
	for (KeyValue kv : result.raw()) {
	System.out.println("Row: " + Bytes.toString(kv.getRow()) +
	" Value: " + Bytes.toString(kv.getValue()));
	}
	}
	}
}
