package org.marcus.weather.analysis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class AnalyzeDailyTemps {

	static String tempValsDaily = "USE weather SELECT da.zip, da.occurred_date, df.collected_date, da.high - df.high AS diff FROM (SELECT * FROM daily_actual WHERE delta_high IS NOT NULL)as da INNER JOIN (SELECT * FROM daily_forecast WHERE delta_high IS NOT NULL) as df ON da.occurred_date = df.forecast_date AND da.zip = df.zip";
	static String diffVDHigh1D = "SELECT COUNT(*) \"count\", ROUND(da.high - df.high, 0) AS diff, da.delta_high FROM (SELECT * FROM daily_forecast WHERE daily_forecast.delta_high IS NOT NULL AND DATEDIFF(DAY,daily_forecast.collected_date,daily_forecast.forecast_date) = 1) AS df INNER JOIN (SELECT * FROM daily_actual) AS da ON df.forecast_date = da.occurred_date AND df.zip = da.zip AND df.high IS NOT NULL AND da.high IS NOT NULL AND da.delta_high IS NOT NULL GROUP BY ROUND(da.high - df.high, 0), da.delta_high ORDER BY diff ASC";
	static String diffVDHigh3D = "SELECT COUNT(*) \"count\", ROUND(da.high - df.high, 0) AS diff, da.delta_high FROM (SELECT * FROM daily_forecast WHERE daily_forecast.delta_high IS NOT NULL AND DATEDIFF(DAY,daily_forecast.collected_date,daily_forecast.forecast_date) = 3) AS df INNER JOIN (SELECT * FROM daily_actual) AS da ON df.forecast_date = da.occurred_date AND df.zip = da.zip AND df.high IS NOT NULL AND da.high IS NOT NULL AND da.delta_high IS NOT NULL GROUP BY ROUND(da.high - df.high, 0), da.delta_high ORDER BY diff ASC";
	static String diffVDHighNP1D = "SELECT COUNT(*) \"count\", ROUND(da.high - df.high, 0) AS diff, da.delta_high FROM (SELECT * FROM daily_forecast WHERE daily_forecast.delta_high IS NOT NULL AND DATEDIFF(DAY,daily_forecast.collected_date,daily_forecast.forecast_date) = 1) AS df INNER JOIN (SELECT * FROM daily_actual WHERE precipitation = 0) AS da ON df.forecast_date = da.occurred_date AND df.zip = da.zip AND df.high IS NOT NULL AND da.high IS NOT NULL AND da.delta_high IS NOT NULL GROUP BY ROUND(da.high - df.high, 0), da.delta_high ORDER BY diff ASC";
	static String diffVDHighP1D = "SELECT COUNT(*) \"count\", ROUND(da.high - df.high, 0) AS diff, da.delta_high FROM (SELECT * FROM daily_forecast WHERE daily_forecast.delta_high IS NOT NULL AND DATEDIFF(DAY,daily_forecast.collected_date,daily_forecast.forecast_date) = 1) AS df INNER JOIN (SELECT * FROM daily_actual WHERE precipitation = 1) AS da ON df.forecast_date = da.occurred_date AND df.zip = da.zip AND df.high IS NOT NULL AND da.high IS NOT NULL AND da.delta_high IS NOT NULL GROUP BY ROUND(da.high - df.high, 0), da.delta_high ORDER BY diff ASC";
	static String diffVDHighNP3D = "SELECT COUNT(*) \"count\", ROUND(da.high - df.high, 0) AS diff, da.delta_high FROM (SELECT * FROM daily_forecast WHERE daily_forecast.delta_high IS NOT NULL AND DATEDIFF(DAY,daily_forecast.collected_date,daily_forecast.forecast_date) = 3) AS df INNER JOIN (SELECT * FROM daily_actual WHERE precipitation = 0) AS da ON df.forecast_date = da.occurred_date AND df.zip = da.zip AND df.high IS NOT NULL AND da.high IS NOT NULL AND da.delta_high IS NOT NULL GROUP BY ROUND(da.high - df.high, 0), da.delta_high ORDER BY diff ASC";
	static String diffVDHighP3D = "SELECT COUNT(*) \"count\", ROUND(da.high - df.high, 0) AS diff, da.delta_high FROM (SELECT * FROM daily_forecast WHERE daily_forecast.delta_high IS NOT NULL AND DATEDIFF(DAY,daily_forecast.collected_date,daily_forecast.forecast_date) = 3) AS df INNER JOIN (SELECT * FROM daily_actual WHERE precipitation = 1) AS da ON df.forecast_date = da.occurred_date AND df.zip = da.zip AND df.high IS NOT NULL AND da.high IS NOT NULL AND da.delta_high IS NOT NULL GROUP BY ROUND(da.high - df.high, 0), da.delta_high ORDER BY diff ASC";
	static String distinctHighCount = "SELECT COUNT(DISTINCT delta_high) FROM weather.dbo.daily_actual";
	static String distinctHighChange = "SELECT DISTINCT delta_high FROM weather.dbo.daily_actual WHERE delta_high IS NOT NULL ORDER BY delta_high";
	static String highErrorHistogram = "SELECT dfpres.delta_high AS high_error, COUNT(*) AS count FROM weather.dbo.daily_forecast AS dfpres, weather.dbo.daily_actual AS dapres WHERE dfpres.zip = dapres.zip AND dapres.occurred_date = dfpres.forecast_date AND dfpres.forecast_date = DATEADD(d,1,dfpres.collected_date) AND dfpres.delta_high IS NOT NULL AND dapres.delta_high = ? GROUP BY dfpres.delta_high";
	static String columnsInHistogram = "SELECT COUNT(DISTINCT dfpres.delta_high) FROM weather.dbo.daily_forecast AS dfpres, weather.dbo.daily_actual AS dapres WHERE dfpres.zip = dapres.zip AND dapres.occurred_date = dfpres.forecast_date AND dfpres.forecast_date = DATEADD(d,1,dfpres.collected_date) AND dfpres.delta_high IS NOT NULL AND dapres.delta_high = ?";

	/**
	 * @param args
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	@SuppressWarnings("unused")
	public static void main(String[] args) throws ClassNotFoundException,
			SQLException {
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		String connectionURL = "jdbc:sqlserver://FRENUM\\SQLEXPRESS;integratedSecurity=true;databaseName=weather;";
		Connection con = DriverManager.getConnection(connectionURL);
		Statement stmt = con.createStatement();
		final String date = new SimpleDateFormat("yyyyMMdd").format(Calendar
				.getInstance().getTime());

	}
}