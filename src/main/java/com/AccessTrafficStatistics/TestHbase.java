package com.AccessTrafficStatistics;




public class TestHbase {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		hbaseoperation hbase = new hbaseoperation();
		hbase.initconnection();
		//hbase.createTable();
		//hbase.insert();
		hbase.queryTable();
	}

}
