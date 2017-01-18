package Hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class GetExample {
	public static void main(String[] args) throws IOException {	
	Configuration conf = HBaseConfiguration.create();
	@SuppressWarnings("deprecation")
	HTable table = new HTable(conf, "testtable");
	Get get = new Get(Bytes.toBytes("row1"));
	get.setMaxVersions(4);
	get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"));
	Result result = table.get(get);
	
	byte[] val = result.getValue(Bytes.toBytes("colfam1"),
	Bytes.toBytes("qual2"));
	System.out.println("Value: " + Bytes.toString(val));
	}
}
