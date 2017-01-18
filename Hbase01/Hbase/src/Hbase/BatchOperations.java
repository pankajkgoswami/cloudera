package Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BatchOperations {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "testtable");
		
		final byte[] ROW1 = Bytes.toBytes("row1");
		final byte[] ROW2 = Bytes.toBytes("row2");
		final byte[] COLFAM1 = Bytes.toBytes("colfam1");
		final byte[] COLFAM2 = Bytes.toBytes("colfam2");
		final byte[] QUAL1 = Bytes.toBytes("qual1");
		final byte[] QUAL2 = Bytes.toBytes("qual2");
		
		List<Row> batch = new ArrayList<Row>();
		
		Put put = new Put(ROW2);
		put.add(COLFAM2, QUAL1, Bytes.toBytes("val5"));
		batch.add(put);
		
		
		Get get1 = new Get(ROW1);
		get1.addColumn(COLFAM1, QUAL1);
		batch.add(get1);
		
		
		Delete delete = new Delete(ROW1);
		delete.deleteColumns(COLFAM1, QUAL1);
		batch.add(delete);
		
		Get get2 = new Get(ROW2);
		get2.addFamily(Bytes.toBytes("BOGUS"));
		batch.add(get2);
		
	
		Object[] results = new Object[batch.size()];
		try {
		table.batch(batch, results);
		} catch (Exception e) {
		System.err.println("Error: " + e);
		}
		for (int i = 0; i < results.length; i++) {
		System.out.println("Result[" + i + "]: " + results[i]);
		}
	}

}
