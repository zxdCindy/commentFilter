package question3;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Populate users.xml into HBase
 * @author cindyzhang
 *
 */
public class PopulateData {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();
		
		
		//Create Table
		HBaseAdmin admin = new HBaseAdmin(config);
		HTableDescriptor htd = new HTableDescriptor("test");
		HColumnDescriptor hcd = new HColumnDescriptor("data");
		htd.addFamily(hcd);
		try{						
			admin.createTable(htd);
		}catch(TableExistsException ex){
			admin.disableTable(htd.getName());
			admin.deleteTable(htd.getName());
			admin.createTable(htd);
		}		
		byte[] tablename = htd.getName();
		HTableDescriptor[] tables = admin.listTables();
		if(tables.length != 1 && Bytes.equals(tablename, tables[0].getName()))
			throw new IOException("Failed create of table");
		
		
		
		//Put data into the table
		HTable table = new HTable(config, htd.getName());
		Path path = Paths.get("/Users/cindyzhang/InputFile/userpost/users.xml");
		try(Scanner scanner = new Scanner(path, "UTF-8")){
			while(scanner.hasNextLine()){
				String line = scanner.nextLine();
				Map<String, String> parsed = MyUtility.transformXmlToMap(line);
				if(parsed.containsKey("Reputation")){
					String userId = parsed.get("Id");
					byte[] row = Bytes.toBytes(userId);				
					Put p1 = new Put(row);
					byte[] databytes = Bytes.toBytes("data");
					p1.add(databytes, Bytes.toBytes("1"), Bytes.toBytes(line));
					table.put(p1);
				}
			}
		}
	}
	
	/**
	 * Read a record from HTable
	 * @param table
	 * @param rowKey
	 * @throws IOException
	 */
	public void readRecord(HTable table, String rowKey) throws IOException{
		Get g = new Get(Bytes.toBytes("2"));
		Result result = table.get(g);
		String value = new String
				(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("1")), "UTF-8");
		System.out.println(value);//Do the process on this record
	}
	
	/**
	 * Read all the records in a HTable
	 * @param table
	 * @throws IOException
	 */
	public void readAllRecords(HTable table) throws IOException{
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		try{
			for(Result scannerResult: scanner){
				Get g = new Get(scannerResult.getRow());
				byte[] rowValue = scannerResult.getValue(Bytes.toBytes("data"), Bytes.toBytes("1"));
				System.out.println("Scan:"+new String(rowValue, "UTF-8"));
			}
			
		}finally{
			scanner.close();
		}
	}

}
