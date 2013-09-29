package org.marcus.weather.process;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.marcus.weather.WeatherTerm;

public class DetermineDeltaHigh implements Runnable{

	static final String DF_QUERY = "SELECT dfpres.high AS forecast_high, dapast.high AS actual_high, dfpres.delta_high AS delta_high FROM weather.dbo.daily_forecast AS dfpres, weather.dbo.daily_actual AS dapast WHERE dfpres.delta_high IS NULL AND dfpres.zip = dapast.zip AND dfpres.forecast_date = dapast.occurred_date ORDER BY dfpres.zip";
	static final String DA_QUERY = "SELECT dapres.high AS pres_high, dapast.high AS past_high, dapres.delta_high AS delta_high FROM weather.dbo.daily_actual AS dapres, weather.dbo.daily_actual AS dapast WHERE dapast.occurred_date = DATEADD(d,-1,dapres.occurred_date) AND dapres.zip = dapast.zip AND dapres.delta_high IS NULL ORDER BY dapast.zip";
	private final int threadNumber;

	private final WeatherTerm wt;

	public DetermineDeltaHigh(WeatherTerm weatherTerm, int threadNumber) {
		wt = weatherTerm;
		this.threadNumber = threadNumber;
	}
	
	/**
	 * @param args
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public void run() {
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String connectionURL = "jdbc:sqlserver://FRENUM\\SQLEXPRESS;integratedSecurity=true;databaseName=weather;";
		Connection con;
		try {
			con = DriverManager.getConnection(connectionURL);
		Statement findDupStmt = con.createStatement(
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		ResultSet rs = findDupStmt.executeQuery(DF_QUERY);
		con.setAutoCommit(false);

		// delta_high for actual is the change from the previous day. delta_high
		// for the forecast is the change predicted from the previous day's
		// actual

		int i = 0;
		for (int q = 0; q < 2; q++) {
			String dhs = "";
			while (rs.next()) {
				final int delta_high = rs.getInt(1) - rs.getInt(2);
				if (rs.getObject(1) == null || rs.getObject(2) == null) {
					rs.updateNull(3);
				} else {
					rs.updateInt(3, delta_high);
					dhs = dhs+" "+delta_high;
				}
				rs.updateRow();
				i++;
				if (i % 500 == 0) {
					con.commit();
					wt.threadOutMessage(dhs, 1, 2);
					dhs = "";
					if (wt.isStop()){
						rs.close();
						con.close();
						return;
					}
				}
			}
			wt.threadOutMessage(dhs, threadNumber, 2);
			rs.close();
			con.commit();
			rs = findDupStmt.executeQuery(DA_QUERY);
			if (wt.isStop()){
				con.close();
				return;
			}
		}
		con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public synchronized boolean isAlive() {
		return Thread.currentThread().isAlive();
	}
	
}
