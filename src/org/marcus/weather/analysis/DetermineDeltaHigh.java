package org.marcus.weather.analysis;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;

public class DetermineDeltaHigh {

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

		String zip = null;
		Integer todayHigh = null;
		Date currDate = null;
		Integer twoDayHigh = null;
		Date currTwoDayDate = null;
		SimpleDateFormat ymdFormatter = new SimpleDateFormat("yyyy-MM-dd");

		// should add something to check if it really is the next day before
		// assigning a delta in the database
		// to take care of breaks in the data

		while (rs.next()) {
			// if new zip code...
			if (zip == null)
				zip = rs.getString(1);
			if (!zip.equals(rs.getString(1))) {
				// if a new zip code, start from the beginning
				zip = rs.getString(1);
				todayHigh = null;
				currDate = null;
				twoDayHigh = null;
				currTwoDayDate = null;
			}
			if (currDate != null) {
				String datePlusOneFormatted = ymdFormatter.format(new Date(
						currDate.getTime() + 86400000));
				// check to make sure it's actually the next day
				if (ymdFormatter.format(rs.getDate(4)).equals(
						datePlusOneFormatted)) {
					if (todayHigh == null) {
						// if yesterday's high was null, update delta_high
						// to
						// null
						rs.updateNull(7);
						todayHigh = rs.getInt(5);
					} else {
						// otherwise update delta_high to be the difference
						rs.updateInt(7, rs.getInt(5) - todayHigh);
						todayHigh = rs.getInt(5);
					}
				} else {
					// if the date is wrong, pretend it is just another new
					// day pair
					todayHigh = rs.getInt(5);
					rs.updateNull(7);
				}
			} else {
				// if the date is null, pretend it is just another new day
				// pair
				todayHigh = rs.getInt(5);
				rs.updateNull(7);
			}
			// update the date, make changes to the row, and increment rows
			currDate = rs.getDate(4);
			rs.updateRow();
			rs.next();
			// do the two day part
			if (currTwoDayDate != null) {
				String currTwoDayDateFormatted = ymdFormatter.format(new Date(
						currTwoDayDate.getTime() + 86400000));
				// check to make sure it's actually the next day
				if (ymdFormatter.format(rs.getDate(4)).equals(
						currTwoDayDateFormatted)) {
					if (twoDayHigh == null) {
						// if yesterday's high was null, update delta_high
						// to
						// null
						rs.updateNull(7);
						twoDayHigh = rs.getInt(5);
					} else {
						// otherwise update delta_high to be the difference
						rs.updateInt(7, rs.getInt(5) - twoDayHigh);
						twoDayHigh = rs.getInt(5);
					}
				} else {
					// if the date is wrong, pretend it is just another new
					// day
					// pair
					twoDayHigh = rs.getInt(5);
					rs.updateNull(7);
				}
			} else {
				// if the date is null, pretend it is just another new day
				// pair
				twoDayHigh = rs.getInt(5);
				rs.updateNull(7);
			}
			// update the date and row
			currTwoDayDate = rs.getDate(4);
			rs.updateRow();
		}
		// close and commit
		rs.close();
		con.commit();
		// repeat with the daily_actual query
		rs = findDupStmt.executeQuery(DA_QUERY);
		while (rs.next()) {
			// if new zip code...
			if (zip == null)
				zip = rs.getString(1);
			if (!zip.equals(rs.getString(1))) {
				// if a new zip code, start from the beginning
				zip = rs.getString(1);
				todayHigh = null;
				currDate = null;
			}
			if (currDate != null) {
				String datePlusOneFormatted = ymdFormatter.format(new Date(
						currDate.getTime() + 86400000));
				// check to make sure it's actually the next day
				if (ymdFormatter.format(rs.getDate(4)).equals(
						datePlusOneFormatted)) {
					if (todayHigh == null) {
						// if yesterday's high was null, update delta_high
						// to null
						rs.updateNull(7);
						todayHigh = rs.getInt(5);
					} else {
						// otherwise update delta_high to be the difference
						rs.updateInt(7, rs.getInt(5) - todayHigh);
						todayHigh = rs.getInt(5);
					}
				} else {
					// if the date is wrong, pretend it is just another new
					todayHigh = rs.getInt(5);
					rs.updateNull(7);
				}
			} else {
				// if the date is null, pretend it is just another new day
				todayHigh = rs.getInt(5);
				rs.updateNull(7);
			}
			// update the date, make changes to the row, and increment rows
			currDate = rs.getDate(4);
			rs.updateRow();
		}
		con.commit();
		con.close();
	}
}
