package com.sample.healthcost;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SampleHiveConnection {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	/**
	 * @param args
	 * @throws SQLException
	 */
	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		// replace "hive" here with the name of the user the queries should run
		// as
		Connection con = DriverManager.getConnection(
				"jdbc:hive2://localhost:10000/default", "hive", "");
		Statement stmt = con.createStatement();
		
		//ResultSet res = stmt.executeQuery("SELECT * FROM healthcostbycode");
		//while (res.next()) {
		//	System.out.println(res.getString(3) + "\t" + res.getString(4) + "\t" + res.getString(5) + "\t" + res.getString(6));
		//}
		/**
		String tableName = "treatmentcostbycodestateandcity";
		
		
		stmt.execute("drop table if exists " + tableName);
		stmt.execute("create table " + tableName + " (healthcodekey string, healthcode string, description string, city string, state string, avgpmtamt string, lowpmtamt string, highpmtamt string) COMMENT 'from csv file' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\054'");
		//stmt.execute("CREATE EXTERNAL TABLE " + tableName + " (healthcodekey string, healthcode string, description string, avgpmtamt string, lowpmtamt string, highpmtamt string) COMMENT 'from csv file' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\054' STORED AS TEXTFILE LOCATION '/healthcosthivetabledata/bycode'");
		
		// show tables
		String sql = "show tables '" + tableName + "'";
		System.out.println("Running: " + sql);
		ResultSet res = stmt.executeQuery(sql);
		if (res.next()) {
			System.out.println(res.getString(1));
		}
		// describe table
		sql = "describe " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2));
		}

		// select * query
		sql = "select * from " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(String.valueOf(res.getInt(1)) + "\t"
					+ res.getString(2));
		}
		**/
		
		
		// regular hive query
		ResultSet res = stmt.executeQuery("SELECT * FROM treatmentcostbycodestateandcity");
		while (res.next()) {
			//System.out.println(res.getString(1) + "\t" + res.getString(2) + "\t" + res.getString(3) + "\t" + res.getString(4) + "\t" + res.getString(5) + "\t" + res.getString(6));
			System.out.println(res.getString(3) + "\t" + res.getString(4) + "\t" + res.getString(5) + "\t" + res.getString(6));
		}
		

		stmt.close();
		con.close();
	}
}
