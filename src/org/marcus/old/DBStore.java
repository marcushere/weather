package org.marcus.old;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;

public class DBStore {

	private Connection con;
	private PreparedStatement insertHA;
	private PreparedStatement insertDA;

	public void open() throws ClassNotFoundException, SQLException {
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		String connectionURL = "jdbc:sqlserver://MARCUSHANPC\\SQLEXPRESS;integratedSecurity=true;databaseName=weather;";
		con = DriverManager.getConnection(connectionURL);
		con.setAutoCommit(false);
		insertHA = con
				.prepareStatement("INSERT INTO weather.dbo.hourly_actual (zip,collected_time,collected_date,occurred_date,hour,temp,conditions,precip_amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
		insertDA = con
				.prepareStatement("INSERT INTO weather.dbo.daily_actual (zip,collected_time,collected_date,occurred_date,high,precip_amount) VALUES (?,?,?,?,?,?)");

	}

	public void storeHA(String zip, Date timestamp, String todayDate,
			String occurredDate, int hour, Float hourlyTemp,
			Float hourlyPrecip, String hourlyConditions) throws SQLException {
		insertHA.setString(1, zip);
		insertHA.setTime(2, new java.sql.Time(timestamp.getTime()));
		insertHA.setString(3, todayDate);
		insertHA.setString(4, occurredDate);
		insertHA.setInt(5, hour);
		if (hourlyTemp == null) {
			insertHA.setNull(6, Types.NUMERIC);
		} else {
			insertHA.setFloat(6, hourlyTemp);
		}

		if (hourlyConditions.isEmpty()) {
			insertHA.setNull(7, Types.VARCHAR);
		} else {
			insertHA.setString(7, hourlyConditions);
		}

		if (hourlyPrecip == null) {
			insertHA.setNull(8, Types.NUMERIC);
		} else {
			insertHA.setFloat(8, hourlyPrecip);
		}
		insertHA.executeUpdate();
	}

	public void storeDA(String zip, Date timestamp, String todayDate,
			String occurredDate, Float high, Float precip) throws SQLException {
		insertDA.setString(1, zip);
		insertDA.setTime(2, new java.sql.Time(timestamp.getTime()));
		insertDA.setString(3, todayDate);
		insertDA.setString(4, occurredDate);
		if (high != null) {
			insertDA.setFloat(5, high);
		} else {
			insertDA.setNull(5, java.sql.Types.FLOAT);
		}

		if (precip != null) {
			insertDA.setFloat(6, precip);
		} else {
			insertDA.setNull(6, java.sql.Types.FLOAT);
		}

		insertDA.executeUpdate();
	}

	public void commit() throws SQLException {
		con.commit();
	}

	public void close() throws SQLException {
		insertHA.close();
		insertDA.close();
		con.close();
	}

}
