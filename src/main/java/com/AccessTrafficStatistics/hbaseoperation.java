package com.AccessTrafficStatistics;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
 
 
public class hbaseoperation 
{
    public Connection connection;	//connection object
    public  Admin admin;			//operation object
 
    public void initconnection() throws Exception 
	{   	
        Configuration conf = HBaseConfiguration.create();
        //conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "192.168.25.118");
        //conf.set("hbase.master", "192.168.25.118:60010");
//        Configuration conf = HBaseConfiguration.create();
//		/*
//			File C:\Windows\System32\drivers\etc\hosts shouled be modified.
//			example:
//				10.0.0.3 master
//				10.0.0.2 slave1
//				10.0.0.4 slave2	
//		*/
//        conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
    }
 
    public void createTable() throws IOException 
	{
        System.out.println("[hbaseoperation] start createtable...");
 
        String tableNameString = "table_book";
        TableName tableName = TableName.valueOf(tableNameString);
        if (admin.tableExists(tableName)) 
		{
            System.out.println("[INFO] table exist");
        }
        else
		{
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            hTableDescriptor.addFamily(new HColumnDescriptor("columnfamily_1"));
            hTableDescriptor.addFamily(new HColumnDescriptor("columnfamily_2"));		
            hTableDescriptor.addFamily(new HColumnDescriptor("columnfamily_3"));
            admin.createTable(hTableDescriptor);
        }
 
        System.out.println("[hbaseoperation] end createtable...");
    }
 
    public void insert() throws IOException 
    {
        System.out.println("[hbaseoperation] start insert...");
 
        Table table = connection.getTable(TableName.valueOf("table_book"));
        List<Put> putList = new ArrayList<Put>();
 
        Put put1;
        put1 = new Put(Bytes.toBytes("row1"));
        put1.addColumn(Bytes.toBytes("columnfamily_1"), Bytes.toBytes("name"), Bytes.toBytes("<<Java In Action>>"));
        put1.addColumn(Bytes.toBytes("columnfamily_1"), Bytes.toBytes("price"), Bytes.toBytes("98.50"));
        put1.addColumn(Bytes.toBytes("columnfamily_2"), Bytes.toBytes("author"), Bytes.toBytes("Tom"));
		put1.addColumn(Bytes.toBytes("columnfamily_2"), Bytes.toBytes("version"), Bytes.toBytes("3 thrd"));	
        put1.addColumn(Bytes.toBytes("columnfamily_3"), Bytes.toBytes("discount"), Bytes.toBytes("5%"));
 
        Put put2;
        put2 = new Put(Bytes.toBytes("row2"));
        put2.addColumn(Bytes.toBytes("columnfamily_1"), Bytes.toBytes("name"), Bytes.toBytes("<<C++ Prime>>"));
        put2.addColumn(Bytes.toBytes("columnfamily_1"), Bytes.toBytes("price"), Bytes.toBytes("68.88"));
        put2.addColumn(Bytes.toBytes("columnfamily_2"), Bytes.toBytes("author"), Bytes.toBytes("Jimmy"));
		put2.addColumn(Bytes.toBytes("columnfamily_2"), Bytes.toBytes("version"), Bytes.toBytes("5 thrd"));	
        put2.addColumn(Bytes.toBytes("columnfamily_3"), Bytes.toBytes("discount"), Bytes.toBytes("15%"));
 
		Put put3;
        put3 = new Put(Bytes.toBytes("row3"));
        put3.addColumn(Bytes.toBytes("columnfamily_1"), Bytes.toBytes("name"), Bytes.toBytes("<<Hadoop in Action>>"));
        put3.addColumn(Bytes.toBytes("columnfamily_1"), Bytes.toBytes("price"), Bytes.toBytes("78.92"));
        put3.addColumn(Bytes.toBytes("columnfamily_2"), Bytes.toBytes("author"), Bytes.toBytes("Kitty"));
		put3.addColumn(Bytes.toBytes("columnfamily_2"), Bytes.toBytes("version"), Bytes.toBytes("2 thrd"));	
        put3.addColumn(Bytes.toBytes("columnfamily_3"), Bytes.toBytes("discount"), Bytes.toBytes("20%"));
 
        putList.add(put1);
        putList.add(put2);
        putList.add(put3);
 
        table.put(putList);
	
        System.out.println("[hbaseoperation] start insert...");
    }
 
	public void queryTable() throws IOException 
	{
		System.out.println("[hbaseoperation] start queryTable...");
 
		Table table = connection.getTable(TableName.valueOf("table_book"));
		ResultScanner scanner = table.getScanner(new Scan());
 
		for (Result result : scanner) 
		{
			byte[] row = result.getRow();
			System.out.println("row key is:" + Bytes.toString(row));
 
			List<Cell> listCells = result.listCells();
			for (Cell cell : listCells) 
			{
				System.out.print("family:" + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(),cell.getFamilyLength()));
				System.out.print("\tqualifier:" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
				System.out.print("\tvalue:" + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
				System.out.println("\tTimestamp:" + cell.getTimestamp());
			}
		}
		
		System.out.println("[hbaseoperation] end queryTable...");
	}
 
	public void queryTableByRowKey(String rowkey) throws IOException 
	{
		System.out.println("[hbaseoperation] start queryTableByRowKey...");
 
		Table table = connection.getTable(TableName.valueOf("table_book"));
		Get get = new Get(rowkey.getBytes());
		Result result = table.get(get);
 
		List<Cell> listCells = result.listCells();
		for (Cell cell : listCells) 
		{
			String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
			long timestamp = cell.getTimestamp();
			String family = Bytes.toString(CellUtil.cloneFamily(cell));
			String qualifier	= Bytes.toString(CellUtil.cloneQualifier(cell));
			String value = Bytes.toString(CellUtil.cloneValue(cell));  
 
			System.out.println(" ===> rowKey : " + rowKey + ",  timestamp : " +  timestamp + ", family : " + family + ", qualifier : " + qualifier + ", value : " + value);
		}
 
		System.out.println("[hbaseoperation] end queryTableByRowKey...");	
	}
 
 
	public void queryTableByCondition(String authorName) throws IOException 
	{
		System.out.println("[hbaseoperation] start queryTableByCondition...");
	
		Table table = connection.getTable(TableName.valueOf("table_book"));
		Filter filter = new SingleColumnValueFilter(Bytes.toBytes("columnfamily_2"), Bytes.toBytes("author"),CompareOp.EQUAL, Bytes.toBytes(authorName));
		Scan scan = new Scan();
 
		scan.setFilter(filter);
 
		ResultScanner scanner = table.getScanner(scan);
 
		for (Result result : scanner) 
		{
		    List<Cell> listCells = result.listCells();
		    for (Cell cell : listCells) 
 
			{
		        String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
		        long timestamp = cell.getTimestamp();
		        String family = Bytes.toString(CellUtil.cloneFamily(cell));
		        String qualifier  = Bytes.toString(CellUtil.cloneQualifier(cell));
		        String value = Bytes.toString(CellUtil.cloneValue(cell));  
 
		        System.out.println(" ===> rowKey : " + rowKey + ",  timestamp : " + timestamp + ", family : " + family + ", qualifier : " + qualifier + ", value : " + value);
		    }
		}
 
		System.out.println("[hbaseoperation] end queryTableByCondition...");
    }
 
	public void deleteColumnFamily(String cf) throws IOException
	{
        TableName tableName = TableName.valueOf("table_book");
        admin.deleteColumn(tableName, Bytes.toBytes(cf));
    }	
	
    public void deleteByRowKey(String rowKey) throws IOException
	{
        Table table = connection.getTable(TableName.valueOf("table_book"));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
		queryTable();
    }
 
	public void truncateTable() throws IOException 
	{
		TableName tableName = TableName.valueOf("table_book");
 
		admin.disableTable(tableName);
		admin.truncateTable(tableName, true);
	}
 
	public void deleteTable() throws IOException 
	{
		admin.disableTable(TableName.valueOf("table_book"));
		admin.deleteTable(TableName.valueOf("table_book"));
	}
}