package org.marcus.weather.process;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.marcus.weather.WeatherTerm;

public class DeterminePrecip implements Runnable {

	static String queryHA = "select * from weather.dbo.hourly_actual where precipitation is null";
	static String queryDA = "select * from weather.dbo.daily_actual where precipitation is null";

	private final WeatherTerm wt;
	private final int tID;

	public DeterminePrecip(WeatherTerm wt, int threadID) {
		this.wt = wt;
		this.tID = threadID;
	}

	/**
	 * @param args
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	@Override
	public void run() {
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		}
		String connectionURL = "jdbc:sqlserver://FRENUM\\SQLEXPRESS;integratedSecurity=true;databaseName=weather;";
		Connection con;
		try {
			con = DriverManager.getConnection(connectionURL);

			Statement findDupStmt = con.createStatement(
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			ResultSet rs = findDupStmt.executeQuery(queryHA);
			con.setAutoCommit(false);

			String pps = "";
			while (rs.next()) {

				String cond = rs.getString("conditions");
				try {
					if (cond.contains("Rain") || cond.contains("Snow")
							|| cond.contains("Sleet")
							|| cond.contains("Squalls")
							|| cond.contains("T-Storm")
							|| cond.contains("Drizzle")
							|| cond.contains("Hail") || cond.contains("Sleet")
							|| cond.contains("Thunderstorm")
							|| cond.contains("Ice Pellets")
							|| cond.contains("Wintry Mix")) {
						rs.updateInt("precipitation", 1);
						pps = pps + "1 ";
					} else if (rs.getFloat("precip_amount") > 0.0) {
						rs.updateInt("precipitation", 1);
					} else if (cond.matches("\\d+")) {
						rs.updateNull("conditions");
					} else {
						rs.updateInt("precipitation", 0);
						pps = pps + "0 ";
					}
					rs.updateRow();
					if (rs.getRow() % 1000 == 0) {
						con.commit();
						if (!pps.isEmpty())
							wt.threadOutMessage(pps, tID, 3);
						pps = "";
						if (wt.isStop()) {
							rs.close();
							con.commit();
							con.close();
							return;
						}
					}
				} catch (NullPointerException e) {

				}
			}
			rs.close();
			con.commit();
			if (!pps.isEmpty())
				wt.threadOutMessage(pps, tID, 3);
			pps = "";
			if (wt.isStop()) {
				con.close();
				return;
			}
			rs = findDupStmt.executeQuery(queryDA);
			while (rs.next()) {
				try {
					if (rs.getFloat("precip_amount") > 0.0) {
						rs.updateInt("precipitation", 1);
						pps = pps + "1 ";
					} else {
						rs.updateInt("precipitation", 0);
						pps = pps + "0 ";
					}
					if (rs.getRow() % 1000 == 0) {
						con.commit();
						if (!pps.isEmpty())
							wt.threadOutMessage(pps, tID, 3);
						pps = "";
						if (wt.isStop()) {
							rs.close();
							con.commit();
							con.close();
							return;
						}
					}
					rs.updateRow();
				} catch (NullPointerException e) {

				}
			}
			rs.close();
			con.commit();
			con.close();
			if (!pps.isEmpty())
				wt.threadOutMessage(pps, tID, 3);
			wt.threadOutMessage("precip thread finished", tID, 3);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
	}

	public synchronized boolean isAlive() {
		return Thread.currentThread().isAlive();
	}
}
