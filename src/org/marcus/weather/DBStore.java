package org.marcus.weather;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;

public class DBStore {

	private Connection con;
	private PreparedStatement insertHA;
	private PreparedStatement insertDA;
	private PreparedStatement insertHF;
	private PreparedStatement insertDF;

	public void open() throws ClassNotFoundException, SQLException {
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		String connectionURL = "jdbc:sqlserver://MARCUSHANPC\\SQLEXPRESS;integratedSecurity=true;databaseName=weather;";
		con = DriverManager.getConnection(connectionURL);
		con.setAutoCommit(false);
		insertHA = con
				.prepareStatement("INSERT INTO weather.dbo.tempHA (zip,collected_time,collected_date,occurred_date,hour,temp,conditions,precip_amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
		insertDA = con
				.prepareStatement("INSERT INTO weather.dbo.tempDA (zip,collected_time,collected_date,occurred_date,high,precip_amount,delta_high) VALUES (?,?,?,?,?,?,?)");
		insertHF = con
				.prepareStatement("INSERT INTO weather.dbo.tempHF (zip,collected_time,collected_date,forecast_date,hour,temp,precip_chance) VALUES (?,?,?,?,?,?,?)");
		insertDF = con
				.prepareStatement("INSERT INTO weather.dbo.tempDF (zip,collected_time,collected_date,forecast_date,high,precip_chance,delta_high) VALUES (?,?,?,?,?,?,?)");
	}

	public void storeForecast(ForecastData forecast) throws Exception {
		try {
			if (forecast.hourlyForecast != null) {
				if (forecast.zip.equalsIgnoreCase("denver,co")) forecast.zip = "80201";
				for (int i = 0; i < forecast.hourlyForecast.length; i++) {
					insertHF.setString(1, forecast.zip);
					insertHF.setTime(2, new java.sql.Time(Calendar
							.getInstance().getTime().getTime()));
					insertHF.setString(3, forecast.today);
					insertHF.setString(4, forecast.forecastDate);
					insertHF.setInt(5, forecast.hourlyForecast[i].hour);
					if (forecast.hourlyForecast[i].temp == null) {
						insertHF.setNull(6, java.sql.Types.INTEGER);
					} else {
						insertHF.setInt(6, forecast.hourlyForecast[i].temp);
					}
					if (forecast.hourlyForecast[i].PoP == null) {
						insertHF.setNull(7, java.sql.Types.INTEGER);
					} else {
						insertHF.setInt(7, forecast.hourlyForecast[i].PoP);
					}
					insertHF.executeUpdate();
				}
			}
		} catch (Exception e) {
			if (e.getMessage()!=null) {
				if (!e.getMessage().contains("PRIMARY KEY")) {
					throw e;
				} else if (e.getClass().equals(new NullPointerException())) {

				}
			}
		}
		try {
			insertDF.setString(1, forecast.zip);
			insertDF.setTime(2, new java.sql.Time(Calendar.getInstance()
					.getTime().getTime()));
			insertDF.setString(3, forecast.today);
			insertDF.setString(4, forecast.forecastDate);
			if (forecast.overallForecast.high == null) {
				insertDF.setNull(5, java.sql.Types.INTEGER);
			} else {
				insertDF.setInt(5, forecast.overallForecast.high);
			}
			if (forecast.overallForecast.PoP == null) {
				insertDF.setNull(6, java.sql.Types.INTEGER);
			} else {
				insertDF.setInt(6, forecast.overallForecast.PoP);
			}
			insertDF.setNull(7, java.sql.Types.BOOLEAN);
			insertDF.executeUpdate();
		} catch (Exception e) {
			if (e.getMessage()!=null) {
				if (!e.getMessage().contains("PRIMARY KEY")) {
					throw e;
				} else if (e.getClass().equals(new NullPointerException())) {

				}
			}
		}
	}

	public void storePast(PastData past) throws Exception {
		try {
			if (past.hourlyPast != null) {
				if (past.zip.equalsIgnoreCase("denver,co")) past.zip = "80201";
				for (int i = 0; i < past.hourlyPast.length; i++) {
					insertHA.setString(1, past.zip);
					insertHA.setTime(2, new java.sql.Time(Calendar
							.getInstance().getTime().getTime()));
					insertHA.setString(3, past.today);
					insertHA.setString(4, past.occurredDate);
					insertHA.setInt(5, past.hourlyPast[i].hour);
					if (past.hourlyPast[i].temp == null) {
						insertHA.setNull(6, Types.NUMERIC);
					} else {
						insertHA.setFloat(6, past.hourlyPast[i].temp);
					}

					if (past.hourlyPast[i].conditions.isEmpty()) {
						insertHA.setNull(7, Types.VARCHAR);
					} else {
						insertHA.setString(7, past.hourlyPast[i].conditions);
					}

					if (past.hourlyPast[i].precip == null) {
						insertHA.setNull(8, Types.NUMERIC);
					} else {
						insertHA.setFloat(8, past.hourlyPast[i].precip);
					}
					insertHA.executeUpdate();
				}
			}
		} catch (Exception e) {
			if (e.getMessage()!=null) {
				if (!e.getMessage().contains("PRIMARY KEY")) {
					throw e;
				} else if (e.getClass().equals(new NullPointerException())) {

				}
			}
		}
		try {
			insertDA.setString(1, past.zip);
			insertDA.setTime(2, new java.sql.Time(Calendar.getInstance()
					.getTime().getTime()));
			insertDA.setString(3, past.today);
			insertDA.setString(4, past.occurredDate);
			if (past.overallPast.high != null) {
				insertDA.setFloat(5, past.overallPast.high);
			} else {
				insertDA.setNull(5, java.sql.Types.FLOAT);
			}

			if (past.overallPast.precip != null) {
				insertDA.setFloat(6, past.overallPast.precip);
			} else {
				insertDA.setNull(6, java.sql.Types.FLOAT);
			}
			insertDA.setNull(7, java.sql.Types.BOOLEAN);
			insertDA.executeUpdate();
		} catch (Exception e) {
			if (e.getMessage()!=null) {
				if (!e.getMessage().contains("PRIMARY KEY")) {
					throw e;
				} else if (e.getClass().equals(new NullPointerException())) {

				}
			}
		}
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
