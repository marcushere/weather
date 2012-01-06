package org.marcus.old;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HourlyPredTable {

	String[] hourlyTemp;
	String[] hourlyPrecipChance;
	String[] hours;

	String predictionDate;
	String todayDate;
	String zip;

	HourlyRecord[] records;

	public HourlyPredTable(String[] temps, String[] precips,
			String[] wundergroundHours, String today, String predDate,
			String zipCode) {
		hourlyTemp = temps;
		hourlyPrecipChance = precips;
		hours = wundergroundHours;

		predictionDate = predDate;
		todayDate = today;
		zip = zipCode;

		records = new HourlyRecord[temps.length];

		for (int i = 0; i < temps.length; i++) {
			records[i] = new HourlyRecord(wundergroundHours[i], temps[i],
					precips[i]);
		}
	}

	// where predDate is the date the prediction is for

	public String toString() {
		String ret = "";
		String predString = "";
		for (int i = 0; i < records.length; i++) {
			predString = predString + records[i].time + "; " + records[i].temp
					+ "; " + records[i].conditions + "; ";
		}
		ret = "hourlyPred; " + todayDate + "; " + predictionDate + "; "
				+ predString;
		return ret;
	}

	/*
	 * A constructor using the line of data from the record file
	 */

	public HourlyPredTable(String line, String zipCode) throws Exception {
		String[] split = line.split("; ");
		if (!split[0].equals("hourlyPred")) {
			throw new Exception();
		}
		todayDate = split[1];
		predictionDate = split[2];
		records = new HourlyRecord[(split.length - 3) / 3];
		int j = 0;
		for (int i = 3; i + 2 < split.length; i = i + 3) {
			records[j] = new HourlyRecord(split[i], split[i + 1], split[i + 2]);
			j++;
		}
		zip = zipCode;
	}

	public void updateDB(String timestamp) throws ClassNotFoundException,
			SQLException {
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		String connectionURL = "jdbc:sqlserver://MARCUSHANPC\\SQLEXPRESS;integratedSecurity=true;databaseName=weather;";
		Connection con = DriverManager.getConnection(connectionURL);
		Statement findDupStmt = con.createStatement(
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		if (hours.length > 0 && hourlyTemp.length > 0
				&& hourlyPrecipChance.length > 0) {
			for (int i = 0; i < hourlyTemp.length; i++) {
				try {
					if ((hours[i] == null | hourlyTemp[i] == null | hourlyPrecipChance[i] == null)
							| (hours[i].equals("") | hourlyTemp[i].equals("") | hourlyPrecipChance[i]
									.equals(""))) {
						findDupStmt
								.executeUpdate("INSERT INTO weather.dbo.hourly_forecast (zip,collected_time,collected_date,forecast_date,hour,temp,precip_chance) VALUES ('"
										+ zip
										+ "','"
										+ timestamp
										+ "','"
										+ todayDate
										+ "','"
										+ predictionDate
										+ "','" + hours[i] + "',null,null)");
					} else {
						findDupStmt
								.executeUpdate("INSERT INTO weather.dbo.hourly_forecast (zip,collected_time,collected_date,forecast_date,hour,temp,precip_chance) VALUES ('"
										+ zip
										+ "','"
										+ timestamp
										+ "','"
										+ todayDate
										+ "','"
										+ predictionDate
										+ "','"
										+ hours[i]
										+ "','"
										+ hourlyTemp[i]
										+ "','"
										+ hourlyPrecipChance[i] + "')");
					}
				} catch (Exception e) {

				}
			}
		}
		findDupStmt.close();
		con.close();
	}
}
