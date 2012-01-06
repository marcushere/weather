package org.marcus.old;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DailyPredTable {

	String high;
	String precip;
	String predictionDate;
	String todayDate;
	String zip;

	public DailyPredTable(String[] forecast, String today, String predDate,
			String zipCode) {
		high = forecast[0];
		precip = forecast[1];
		predictionDate = predDate;
		todayDate = today;
		zip = zipCode;
	}

	public String toString() {
		String ret = "";
		String predString = "";
		predString = high + "; " + precip + "; ";
		ret = "dailyPred; " + todayDate + "; " + predictionDate + "; "
				+ predString;
		return ret;
	}

	/*
	 * A constructor using the line of data from the record file
	 */

	public DailyPredTable(String line, String zipCode) throws Exception {
		String[] split = line.split("; ");
		if (!split[0].equals("dailyPred")) {
			throw new Exception();
		}
		todayDate = split[1];
		predictionDate = split[2];
		high = split[3];
		precip = split[4];
		zip = zipCode;
	}

	public void updateDB(String timestamp) throws ClassNotFoundException,
			SQLException {
		String query;
		if ((high == null | precip == null)
				| (high.equals("") | precip.equals(""))) {
			query = "INSERT INTO weather.dbo.daily_forecast (zip,collected_time,collected_date,forecast_date,high,precip_chance) VALUES ('"
					+ zip
					+ "','"
					+ timestamp
					+ "','"
					+ todayDate
					+ "','"
					+ predictionDate + "',null,null)";
		} else {
			query = "INSERT INTO weather.dbo.daily_forecast (zip,collected_time,collected_date,forecast_date,high,precip_chance) VALUES ('"
					+ zip
					+ "','"
					+ timestamp
					+ "','"
					+ todayDate
					+ "','"
					+ predictionDate + "','" + high + "','" + precip + "')";
		}
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		String connectionURL = "jdbc:sqlserver://MARCUSHANPC\\SQLEXPRESS;integratedSecurity=true;databaseName=weather;";
		Connection con = DriverManager.getConnection(connectionURL);
		Statement findDupStmt = con.createStatement(
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		try {
			findDupStmt.executeUpdate(query);
		} catch (Exception e) {

		}
		findDupStmt.close();
		con.close();
	}
}
