package Hbase;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class ScanOperation {

	@SuppressWarnings({ "deprecation", "resource" })
	public static void main(String[] args) throws IOException {
		
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "testtable");
		Scan scan1 = new Scan();
		ResultScanner scanner1 = table.getScanner(scan1);
		for (Result res : scanner1) {
		System.out.println(res);
		}
		scanner1.close();
		System.out.println("***************************************************************");
		
		Scan scan2 = new Scan();
		scan2.addFamily(Bytes.toBytes("colfam1"));
		ResultScanner scanner2 = table.getScanner(scan2);
		for (Result res : scanner2) {
			System.out.println(res);
			}
		scanner2.close();
		System.out.println("***************************************************************");
		
		Scan scan3 = new Scan();
		scan3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("val04")).
		addColumn(Bytes.toBytes("colfam2"), Bytes.toBytes("value-10")).
		setStartRow(Bytes.toBytes("row1")).
		setStopRow(Bytes.toBytes("row2"));
		ResultScanner scanner3 = table.getScanner(scan3);
		for (Result res : scanner3) {
		System.out.println(res);
		}
		scanner3.close();
		
	}

}
