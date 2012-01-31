package org.marcus.weather.analysis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.microsoft.sqlserver.jdbc.SQLServerException;

public class FindDeltaHigh {

	static final String DF_QUERY = "select * from weather.dbo.daily_forecast order by zip, collected_date, forecast_date, collected_time desc";
	static final String DA_QUERY = "select * from weather.dbo.daily_actual order by zip, collected_date, occurred_date, collected_time desc";

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
		ResultSet rs = findDupStmt.executeQuery(DF_QUERY);
		con.setAutoCommit(false);

		rs.next();
		Integer todayHigh = rs.getInt(5);
		rs.next();
		Integer twoDayHigh = rs.getInt(5);
		String zip = rs.getString(1);
		Integer diff = null;
		boolean isNotNull = true;

		while (isNotNull) {
			if (!rs.next())
				break;
			if (!zip.equals(rs.getString(1))) {
				zip = rs.getString(1);
				todayHigh = rs.getInt(5);
				rs.updateNull(7);
				rs.updateRow();
				rs.next();
				twoDayHigh = rs.getInt(5);
				rs.updateNull(7);
				rs.updateRow();
				rs.next();
			}
			try {
				if (twoDayHigh == null) {
					try {
						twoDayHigh = rs.getInt(5);
						rs.updateNull(7);
						rs.updateRow();
						rs.next();
						todayHigh = rs.getInt(5);
						rs.updateNull(7);
						rs.updateRow();
						rs.next();
					} catch (SQLServerException e) {
						break;
					}
				} else {
					diff = rs.getInt(5) - todayHigh;
				}
				if (!rs.wasNull()) {
					rs.updateInt(7, diff);
					todayHigh = rs.getInt(5);
				} else {
					rs.updateNull(7);
					todayHigh = null;
				}
				rs.updateRow();
			} catch (NullPointerException e) {
			}
			isNotNull = rs.next();
			if (isNotNull) {
				try {
					diff = rs.getInt(5) - twoDayHigh;
					if (!rs.wasNull()) {
						rs.updateInt(7, diff);
						twoDayHigh = rs.getInt(5);
					} else {
						rs.updateNull(7);
						twoDayHigh = null;
					}
					rs.updateRow();
				} catch (NullPointerException e) {
				}

			}
		}
		rs.close();
		con.commit();

		rs = findDupStmt.executeQuery(DA_QUERY);

		isNotNull = rs.next();
		zip = rs.getString(1);
		todayHigh = rs.getInt(5);
		isNotNull = rs.next();
		while (isNotNull) {
			if (!zip.equalsIgnoreCase(rs.getString(1))) {
				rs.updateNull(7);
				rs.updateRow();
				zip = rs.getString(1);
				isNotNull = rs.next();
			}
			try {
				if (isNotNull) {
					if (todayHigh == null) {
						rs.updateNull(7);
						rs.updateRow();
						isNotNull = rs.next();
						todayHigh = rs.getInt(5);
					} else {
						diff = rs.getInt(5) - todayHigh;
						rs.updateInt(7, diff);
						rs.updateRow();
						todayHigh = rs.getInt(5);
						if (rs.wasNull()) todayHigh = null;
					}
				}
			} catch (NullPointerException e) {
			}
			isNotNull = rs.next();
		}
		con.commit();
		con.close();
	}
}
