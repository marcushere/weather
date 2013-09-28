package org.marcus.weather.ops;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.marcus.weather.DBStore;

import com.microsoft.sqlserver.jdbc.SQLServerException;

public class ImportTableFromCSV {

	private static final boolean overwriteDatabase = true;
	
	static String HAfilename = "C:\\src\\java\\weather\\weather\\CSV\\HourlyActual.csv";
	static String HFfilename = "C:\\src\\java\\weather\\weather\\CSV\\HourlyForecast.csv";
	static String DAfilename = "C:\\src\\java\\weather\\weather\\CSV\\DailyActual.csv";
	static String DFfilename = "C:\\src\\java\\weather\\weather\\CSV\\DailyForecast.csv";
//	private static final boolean titles = false;

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws ParseException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, SQLException, ParseException,
			InterruptedException {
		DBStore db = new DBStore(overwriteDatabase);
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		String connectionURL = "jdbc:sqlserver://FRENUM\\SQLEXPRESS;integratedSecurity=true;databaseName=weather;";
		Connection con = DriverManager.getConnection(connectionURL);
		con.setAutoCommit(false);
		PreparedStatement insertHA = con
				.prepareStatement("INSERT INTO weather.dbo.hourly_actual (zip,collected_time,collected_date,occurred_date,hour,temp,conditions,precip_amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
		PreparedStatement insertDA = con
				.prepareStatement("INSERT INTO weather.dbo.daily_actual (zip,collected_time,collected_date,occurred_date,high,precip_amount,delta_high) VALUES (?,?,?,?,?,?,?)");
		PreparedStatement insertHF = con
				.prepareStatement("INSERT INTO weather.dbo.hourly_forecast (zip,collected_time,collected_date,forecast_date,hour,temp,precip_chance) VALUES (?,?,?,?,?,?,?)");
		PreparedStatement insertDF = con
				.prepareStatement("INSERT INTO weather.dbo.daily_forecast (zip,collected_time,collected_date,forecast_date,high,precip_chance,delta_high) VALUES (?,?,?,?,?,?,?)");

		SimpleDateFormat timeFormat = new SimpleDateFormat("kk:mm:ss");
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

		// Hourly actual
		// BufferedReader br = new BufferedReader(new FileReader(HAfilename));
		// String line = br.readLine();
		// int n = 0;
		// while (line != null) {
		// System.out.println(line);
		// String[] split = new String[10];
		// int index = 0;
		// for (int i = 0; i < 11; i++) {
		// try {
		// split[i] = line.substring(index, line.indexOf(",", index));
		// index = line.indexOf(",", index) + 1;
		// } catch (StringIndexOutOfBoundsException e) {
		// break;
		// }
		// }
		// insertHA.setString(1, split[0]);
		// insertHA.setString(2, split[1]);
		// insertHA.setString(3, split[2]);
		// insertHA.setString(4, split[3]);
		// insertHA.setString(5, split[4]);
		// if (split[4].isEmpty()) {
		// insertHA.setNull(6, Types.NUMERIC);
		// } else {
		// insertHA.setString(6, split[5]);
		// }
		// if (split[6].isEmpty()) {
		// insertHA.setNull(7, Types.VARCHAR);
		// } else {
		// insertHA.setString(7, split[6]);
		// }
		// if (split[7].isEmpty()) {
		// insertHA.setNull(8, Types.NUMERIC);
		// } else {
		// insertHA.setString(8, split[7]);
		// }
		// line = br.readLine();
		// try {
		// insertHA.executeUpdate();
		// } catch (SQLServerException e) {
		// e.printStackTrace();
		// }
		// n++;
		// if (n%100==0){
		// con.commit();
		// }
		// }

		// daily actual
		/*
		 * BufferedReader br = new BufferedReader(new FileReader(DAfilename));
		 * String line = br.readLine(); while (line != null) {
		 * System.out.println(line); String[] split = new String[10]; int index
		 * = 0; for (int i = 0; i < 11; i++) { try { split[i] =
		 * line.substring(index, line.indexOf(",", index)); index =
		 * line.indexOf(",", index) + 1; } catch
		 * (StringIndexOutOfBoundsException e) { break; } }
		 * insertDA.setString(1, split[0]); insertDA.setTime(2, new
		 * java.sql.Time(timeFormat.parse(split[1]) .getTime()));
		 * insertDA.setDate(3, new java.sql.Date(dateFormat.parse(split[2])
		 * .getTime())); insertDA.setDate(4, new
		 * java.sql.Date(dateFormat.parse(split[3]) .getTime())); if
		 * (split[4].isEmpty()) { insertDA.setNull(5, Types.NUMERIC); } else {
		 * insertDA.setFloat(5, Float.parseFloat(split[4])); } if
		 * (split[5].isEmpty()) { insertDA.setNull(6, Types.NUMERIC); } else {
		 * insertDA.setFloat(6, Float.parseFloat(split[5])); } // if
		 * (split[6].isEmpty()) { insertDA.setNull(7, Types.NUMERIC); // } else
		 * { // insertDA.setFloat(7, Float.parseFloat(split[6])); // } try {
		 * insertDA.executeUpdate(); } catch (SQLServerException e) {
		 * 
		 * } line = br.readLine(); }
		 */

		// Hourly forecast
//		BufferedReader br = new BufferedReader(new FileReader(HFfilename));
//		String line = br.readLine();
//		int n = 0;
//		while (line != null) {
//			System.out.println(line);
//			String[] split = new String[10];
//			int index = 0;
//			for (int i = 0; i < 11; i++) {
//				try {
//					split[i] = line.substring(index, line.indexOf(",", index));
//					index = line.indexOf(",", index) + 1;
//				} catch (StringIndexOutOfBoundsException e) {
//					split[i] = line.substring(index);
//					break;
//				}
//			}
//			insertHF.setString(1, split[0]);
//			insertHF.setString(2, split[1]);
//			insertHF.setString(3, split[2]);
//			insertHF.setString(4, split[3]);
//			insertHF.setString(5, split[4]);
//			if (split[5].isEmpty()) {
//				insertHF.setNull(6, Types.NUMERIC);
//			} else {
//				insertHF.setString(6, split[5]);
//			}
//			if (split[6].isEmpty()) {
//				insertHF.setNull(7, Types.NUMERIC);
//			} else {
//				insertHF.setString(7, split[6]);
//			}
//			line = br.readLine();
//			try {
//				insertHF.executeUpdate();
//			} catch (SQLServerException e) {
//				e.printStackTrace();
//			}
//			n++;
//			if (n % 100 == 0) {
//				con.commit();
//			}
//		}

		// daily forecast
		BufferedReader br = new BufferedReader(new FileReader(DFfilename));
		String line = br.readLine();
		int n = 0;
		while (line != null) {
			System.out.println(line);
			String[] split = new String[10];
			int index = 0;
			for (int i = 0; i < 11; i++) {
				try {
					split[i] = line.substring(index, line.indexOf(",", index));
					index = line.indexOf(",", index) + 1;
				} catch (StringIndexOutOfBoundsException e) {
					split[i] = line.substring(index);
					break;
				}
			}
			insertDF.setString(1, split[0]);
			insertDF.setString(2, split[1]);
			insertDF.setString(3, split[2]);
			insertDF.setString(4, split[3]);
			if (split[4].isEmpty()) {
				insertDF.setNull(5, Types.NUMERIC);
			} else {
				insertDF.setString(5, split[4]);
			}
			if (split[5].isEmpty()) {
				insertDF.setNull(6, Types.NUMERIC);
			} else {
				insertDF.setString(6, split[5]);
			}
			if (split[6].isEmpty()) {
				insertDF.setNull(7, Types.NUMERIC);
			} else {
				insertDF.setString(7, split[6]);
			}
			line = br.readLine();
			try {
				insertDF.executeUpdate();
			} catch (SQLServerException e) {
				e.printStackTrace();
			}
			n++;
			if (n % 100 == 0) {
				con.commit();
			}
		}

		con.commit();
		con.close();
	}
}
