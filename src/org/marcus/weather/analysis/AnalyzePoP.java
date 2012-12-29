package org.marcus.weather.analysis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class AnalyzePoP {

	private static String distinctPoPValsHourly = "select distinct precip_chance from weather.dbo.hourly_forecast order by precip_chance";
	private static String PoPTestHourly = "use weather select count(*) from hourly_forecast inner join hourly_actual on hourly_forecast.forecast_date = hourly_actual.occurred_date and hourly_forecast.zip = hourly_actual.zip and hourly_forecast.hour = hourly_actual.hour where hourly_forecast.precip_chance = ? and hourly_actual.precipitation = ?";
	private static String distinctPoPValsDaily = "select distinct precip_chance from weather.dbo.daily_forecast order by precip_chance";
	private static String PoPTestDaily = "use weather select count(*) from daily_forecast inner join daily_actual on daily_forecast.forecast_date = daily_actual.occurred_date and daily_forecast.zip = daily_actual.zip where daily_forecast.precip_chance = ? and daily_actual.precipitation = ?";
	
	/**
	 * @param args
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		String connectionURL = "jdbc:sqlserver://MARCUSHANPC\\SQLEXPRESS;integratedSecurity=true;databaseName=weather;";
		Connection con = DriverManager.getConnection(connectionURL);
		Statement stmt = con.createStatement();
		ResultSet poPValueSet = stmt.executeQuery(distinctPoPValsHourly);
		
		String[] PoPValues;
		String[] temp = new String[50];
		int i = 0;
		while (poPValueSet.next()) {
			if (!(poPValueSet.getString("precip_chance")==null)) {
				temp[i] = poPValueSet.getString("precip_chance");
				i++;
			}
		}
		PoPValues = new String[i];
		for (int s=0; s<i; s++){
			PoPValues[s] = temp[s];
		}
		
		poPValueSet.close();
		
		double[] chance = new double[PoPValues.length];
		double[] brierScore = new double[PoPValues.length];
		
		PreparedStatement ps = con.prepareStatement(PoPTestHourly, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		for (i=0; i<PoPValues.length; i++){
			ps.setString(1, PoPValues[i]);
			ps.setString(2, "1");
			ResultSet rs = ps.executeQuery();
			rs.next();
			int rainedNumber = rs.getInt(1);
			rs.close();
			ps.setString(2, "0");
			rs = ps.executeQuery();
			rs.next();
			int notRained = rs.getInt(1);
			rs.close();
			chance[i] = (double)rainedNumber/((double)notRained+(double)rainedNumber);
			brierScore[i] = (Math.pow(Double.valueOf(PoPValues[i])/100.0-1.0,2.0)*(double)rainedNumber+Math.pow(Double.valueOf(PoPValues[i])/100.0,2.0)*(double)notRained)/((double)notRained+(double)rainedNumber);
		}
		System.out.println("Hourly forecasts:");
		for (i=0; i<PoPValues.length;i++){
			System.out.println(PoPValues[i]+", "+Double.toString(chance[i])+", "+Double.toString(brierScore[i]));
		}
		
		poPValueSet = stmt.executeQuery(distinctPoPValsDaily);
		
		temp = new String[50];
		i = 0;
		while (poPValueSet.next()) {
			if (!(poPValueSet.getString("precip_chance")==null)) {
				temp[i] = poPValueSet.getString("precip_chance");
				i++;
			}
		}
		PoPValues = new String[i];
		for (int s=0; s<i; s++){
			PoPValues[s] = temp[s];
		}
		
		poPValueSet.close();
		
		chance = new double[PoPValues.length];
		brierScore = new double[PoPValues.length];
		
		ps = con.prepareStatement(PoPTestDaily, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		for (i=0; i<PoPValues.length; i++){
			ps.setString(1, PoPValues[i]);
			ps.setString(2, "1");
			ResultSet rs = ps.executeQuery();
			rs.next();
			int rainedNumber = rs.getInt(1);
			rs.close();
			ps.setString(2, "0");
			rs = ps.executeQuery();
			rs.next();
			int notRained = rs.getInt(1);
			rs.close();
			chance[i] = (double)rainedNumber/((double)notRained+(double)rainedNumber);
			brierScore[i] = (Math.pow(Double.valueOf(PoPValues[i])/100.0-1.0,2.0)*(double)rainedNumber+Math.pow(Double.valueOf(PoPValues[i])/100.0,2.0)*(double)notRained)/((double)notRained+(double)rainedNumber);
		}
		System.out.println("Daily forecasts:");
		for (i=0; i<PoPValues.length;i++){
			System.out.println(PoPValues[i]+", "+Double.toString(chance[i])+", "+Double.toString(brierScore[i]));
		}
	}
}
