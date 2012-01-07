package org.marcus.weather.analysis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SQLTry {

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

		List<String> zips = new ArrayList<String>();

		PreparedStatement ps = con
				.prepareStatement("select count(*) from daily_forecast group by zip, collected_date, occurred_date, hour");
		Statement stmt = con.createStatement();
		ResultSet rs = stmt
				.executeQuery("select distinct zip from daily_forecast");
		while (rs.next()) {
			String ar = rs.getString("zip");
			zips.add(ar);
			System.out.println(ar);
		}
		rs.close();

		for (String zip : zips) {
			// ps.setString(1, zip);
			rs = ps.executeQuery();
			// rs.deleteRow();
			rs.next();
			System.out.println("found " + rs.getInt(1) + " readings in " + zip);
			rs.close();
		}

		ps.close();
		stmt.close();
		con.close();
	}

}
