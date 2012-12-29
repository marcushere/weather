package org.marcus.weather.analysis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DetermineDeltaHigh {

	static final String DF_QUERY = "SELECT dfpres.high AS forecast_high, dapast.high AS actual_high, dfpres.delta_high AS delta_high FROM weather.dbo.daily_forecast AS dfpres, weather.dbo.daily_actual AS dapast WHERE dfpres.delta_high IS NULL AND dfpres.zip = dapast.zip AND dfpres.forecast_date = dapast.occurred_date ORDER BY dfpres.zip";
	static final String DA_QUERY = "SELECT dapres.high AS pres_high, dapast.high AS past_high, dapres.delta_high AS delta_high FROM weather.dbo.daily_actual AS dapres, weather.dbo.daily_actual AS dapast WHERE dapast.occurred_date = DATEADD(d,-1,dapres.occurred_date) AND dapres.zip = dapast.zip AND dapres.delta_high IS NULL ORDER BY dapast.zip";

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

		// delta_high for actual is the change from the previous day. delta_high
		// for the forecast is the change predicted from the previous day's
		// actual

		int i = 0;
		for (int q = 0; q < 2; q++) {
			while (rs.next()) {

				final int delta_high = rs.getInt(1) - rs.getInt(2);
				if (rs.getObject(1) == null || rs.getObject(2) == null) {
					rs.updateNull(3);
				} else {
					rs.updateInt(3, delta_high);
					System.out.print(delta_high);
				}
				rs.updateRow();
				i++;
				if (i % 1000 == 0) {
					con.commit();
					System.out.println();
				}
			}
			con.commit();
			rs.close();
			rs = findDupStmt.executeQuery(DA_QUERY);
			System.out.println();
			System.out.println();
		}
		con.close();
	}
}
