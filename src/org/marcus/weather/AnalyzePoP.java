package org.marcus.weather;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class AnalyzePoP {

	private static String distinctPoPVals = "select distinct precip_chance from weather.dbo.hourly_forecast order by precip_chance";
	private static String PoPTest = "use weather select hourly_forecast.zip,hourly_forecast.precip_chance,hourly_actual.conditions,hourly_actual.precipitation,hourly_forecast.collected_date,hourly_forecast.forecast_date from hourly_forecast inner join hourly_actual on hourly_forecast.forecast_date = hourly_actual.occurred_date and hourly_forecast.zip = hourly_actual.zip and hourly_forecast.hour = hourly_actual.hour where hourly_forecast.precip_chance = ? and hourly_actual.precipitation = ?";

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
		ResultSet poPValueSet = stmt.executeQuery(distinctPoPVals);
		
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
		double[] pVal = new double[PoPValues.length];
		
		PreparedStatement ps = con.prepareStatement(PoPTest, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		for (i=0; i<PoPValues.length; i++){
			ps.setString(1, PoPValues[i]);
			ps.setString(2, "1");
			ResultSet rs = ps.executeQuery();
			int rainedNumber = 0;
			while (rs.next()){
				rainedNumber = rs.getRow();
			}
			rs.close();
			ps.setString(2, "0");
			rs = ps.executeQuery();
			int notRained = 0;
			while (rs.next()){
				notRained = rs.getRow();
			}
			rs.close();
			chance[i] = (double)rainedNumber/((double)notRained+(double)rainedNumber);
			pVal[i] = (Math.pow(Double.valueOf(PoPValues[i])/100.0-1.0,2.0)*(double)rainedNumber+Math.pow(Double.valueOf(PoPValues[i])/100.0,2.0)*(double)notRained)/((double)notRained+(double)rainedNumber);
		}
		for (i=0; i<PoPValues.length;i++){
			System.out.println(PoPValues[i]+", "+Double.toString(chance[i])+", "+Double.toString(pVal[i]));
		}
	}
}
