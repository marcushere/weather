package org.marcus.weather.analysis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class RemoveDuplicateRows {

	/**
	 * @param args
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public static void main(String[] args) throws ClassNotFoundException,
			SQLException {
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		String connectionURL = "jdbc:sqlserver://MARCUSHANPC\\SQLEXPRESS;integratedSecurity=true;databaseName=weather;";
		Connection con = DriverManager.getConnection(connectionURL);
		Statement findDupStmt = con.createStatement(
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		ResultSet rs = findDupStmt
				.executeQuery("select * from weather.dbo.daily_actual order by zip, collected_date, occurred_date, collected_time desc");

		String zip = "";
		String collDate = "";
		String occDate = "";
//		String hour = "";

		int i = 0;

		while (rs.next()) {
			try {
				if (rs.getString("zip").equals(zip)
				&& rs.getString("collected_date").equals(collDate)
						&& rs.getString("occurred_date").equals(occDate)){
//						&& rs.getString("hour").equals(hour)) {
					rs.deleteRow();
					i++;
				} else {
					zip = rs.getString("zip");
					collDate = rs.getString("collected_date");
					occDate = rs.getString("occurred_date");
//					hour = rs.getString("hour");
				}
			} catch (Exception e) {

			}
		}
		System.out.println(i);
	}

}
