package org.marcus.weather.analysis;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ExportTableAsCSV {

	static String queryDF = "select * from weather.dbo.daily_forecast";
	static String filenameDF = "C:\\Users\\Marcus\\Documents\\Dropbox\\weather\\data\\daily_forecast";
	static String queryHF = "select * from weather.dbo.hourly_forecast";
	static String filenameHF = "C:\\Users\\Marcus\\Documents\\Dropbox\\weather\\data\\hourly_forecast";
	static String queryDA = "select * from weather.dbo.daily_actual";
	static String filenameDA = "C:\\Users\\Marcus\\Documents\\Dropbox\\weather\\data\\daily_actual";
	static String queryHA = "select * from weather.dbo.hourly_actual";
	static String filenameHA = "C:\\Users\\Marcus\\Documents\\Dropbox\\weather\\data\\hourly_actual";
	
	static boolean titles = false;

	/**
	 * @param args
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws IOException
	 */
	public static void main(String[] args) throws ClassNotFoundException,
			SQLException, IOException {
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		String connectionURL = "jdbc:sqlserver://MARCUSHANPC\\SQLEXPRESS;integratedSecurity=true;databaseName=weather;";
		Connection con = DriverManager.getConnection(connectionURL);
		Statement findDupStmt = con.createStatement();

		System.out.println(queryDF);
		writeFile(findDupStmt, queryDF, addDateAndExt(filenameDF), titles);
		System.out.println(queryHF);
		writeFile(findDupStmt, queryHF, addDateAndExt(filenameHF), titles);
		System.out.println(queryDA);
		writeFile(findDupStmt, queryDA, addDateAndExt(filenameDA), titles);
		System.out.println(queryHA);
		writeFile(findDupStmt, queryHA, addDateAndExt(filenameHA), titles);
	}

	private static String addDateAndExt(String filename) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		return filename+sdf.format(new Date())+".csv";
	}

	/**
	 * @param findDupStmt
	 * @throws SQLException
	 * @throws IOException
	 */
	private static void writeFile(Statement findDupStmt,String sqlquery, String fileName, boolean printTitles) throws SQLException,
			IOException {
		ResultSet rs = findDupStmt.executeQuery(sqlquery);

		ResultSetMetaData rsmd = rs.getMetaData();

		FileWriter fileWriter = new FileWriter(fileName, false);
		PrintWriter out = new PrintWriter(fileWriter, true);

		String line = "";
		
		if (printTitles) {
			line = rsmd.getColumnName(1);
			for (int i = 2; i == rsmd.getColumnCount(); i++) {
				line = line + "," + rsmd.getColumnName(i);
			}
			out.println(line);
			line = "";
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		while (rs.next()) {
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {
				try {
					if (i == 1) {
						line = rs.getString(i);
					} else if (rs.getString(i).equals("null")){
						line = line + ",";
					} else if (i==3 || i==4){
						line = line + "," + sdf.format(new Date(rs.getDate(i).getTime()+3600*24*2*1000));
					} else {
						line = line + "," + rs.getString(i);
					}
				} catch (NullPointerException e) {
					line = line + ",";
				}
			}
			line = line + ",";
			out.println(line);
			line = "";
		}

		out.close();
		fileWriter.close();
	}

}
