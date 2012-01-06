package org.marcus.weather;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DeterminePrecip {

	static String query = "select * from weather.dbo.hourly_actual";

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
		ResultSet rs = findDupStmt.executeQuery(query);
		con.setAutoCommit(false);

		while (rs.next()) {
			String cond = rs.getString("conditions");
			try {
				if (cond.contains("Rain") || cond.contains("Snow")
						|| cond.contains("Sleet") || cond.contains("Squalls")
						|| cond.contains("T-Storm") || cond.contains("Drizzle")
						|| cond.contains("Hail") || cond.contains("Sleet")
						|| cond.contains("Thunderstorm")
						|| cond.contains("Ice Pellets")
						|| cond.contains("Wintry Mix")) {
					rs.updateInt("precipitation", 1);
					System.out.println("1");
				} else if (rs.getFloat("precip_amount") > 0.0) {
					rs.updateInt("precipitation", 1);
				} else if (cond.matches("\\d+")) {
					rs.updateNull("conditions");
				} else {
					rs.updateInt("precipitation", 0);
					System.out.println("0");
				}
				rs.updateRow();
				if (rs.getRow() % 1000 == 0) {
					con.commit();
				}
			} catch (NullPointerException e) {

			}
		}
		rs.close();
		con.commit();
		con.close();
	}
}
