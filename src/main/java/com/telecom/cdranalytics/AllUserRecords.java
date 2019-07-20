package com.telecom.cdranalytics;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import java.util.List;

public class AllUserRecords {

	public static void main(String args[]) throws IOException{

	      Configuration config = HBaseConfiguration.create();
	      HTable table = new HTable(config, "promotedFreePackUsers");

	      // instantiate the Scan class
	      Scan scan = new Scan();

	      // scan the columns
	      scan.addColumn(Bytes.toBytes("callerStatistic"), Bytes.toBytes("duration"));
	 
	      // get the ResultScanner
	      ResultScanner scanner = table.getScanner(scan);
	      System.out.println("============================ All User Records ============================");	
	      for (Result result = scanner.next(); result != null; result=scanner.next()){
		  List<Cell> cells = result.listCells();
		    for (Cell cell : cells) {
			String mobileNumber = Bytes.toString(CellUtil.cloneRow(cell));
			long totalDuration = Bytes.toLong(CellUtil.cloneValue(cell));
			System.out.println("mobile no="  + mobileNumber + " Total call duration time (in minutes)=" + totalDuration);
			      
		    }

	       }
	  System.out.println("========================================================================");	       
	      scanner.close();
	   }
}
