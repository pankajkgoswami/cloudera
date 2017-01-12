import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class PutExample
{
  
public static void main(String[] args) throws IOException
  {
    Configuration hconfig = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(hconfig); 
    Admin hbase_admin = connection.getAdmin();
    HTableDescriptor htable = new HTableDescriptor(TableName.valueOf("User1")); 
    htable.addFamily( new HColumnDescriptor("Id"));
    htable.addFamily( new HColumnDescriptor("Name"));
    System.out.println( "Connecting..." );
  
    System.out.println( "Creating Table..." );
    hbase_admin.createTable( htable );
    System.out.println("Done!");
  }
}