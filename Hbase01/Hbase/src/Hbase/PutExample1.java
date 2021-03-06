package Hbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

	import java.io.IOException;
	public class PutExample1 {
	public static void main(String[] args) throws IOException {
	Configuration conf = HBaseConfiguration.create();
	HTable table = new HTable(conf, "testtable");
	Put put = new Put(Bytes.toBytes("row1"));
	put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
	Bytes.toBytes("val11"));
	put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
	Bytes.toBytes("val12"));
	table.put(put);
	}
	}