package Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class DeleteData {
	public static void main(String[] args) throws IOException {
	Configuration conf = HBaseConfiguration.create();
	@SuppressWarnings("deprecation")
	HTable table = new HTable(conf, "testtable");
	
	Delete delete = new Delete(Bytes.toBytes("row1"));
//	delete.setTimestamp(1);
//	delete.deleteColumn(Bytes.toBytes("colfam2"), Bytes.toBytes("q1"), 1484731512184L);
//	delete.deleteColumn(Bytes.toBytes("colfam2"), Bytes.toBytes("qual1"));
//	delete.deleteColumns(Bytes.toBytes("colfam2"), Bytes.toBytes("qual1"));
	
	delete.deleteColumns(Bytes.toBytes("colfam2"), Bytes.toBytes("q1"), 1484731512184L);
//	delete.deleteFamily(Bytes.toBytes("colfam2"));
//	delete.deleteFamily(Bytes.toBytes("colfam2"), 1484730770779L);
	table.delete(delete);
	table.close();
	}
}
