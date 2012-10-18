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

	static String query = "select * from weather.dbo.hourly_forecast";
	static String filename = "C:\\Users\\Marcus\\Documents\\Dropbox\\weather\\data\\hourly_forecast.csv";
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
		ResultSet rs = findDupStmt.executeQuery(query);

		ResultSetMetaData rsmd = rs.getMetaData();

		FileWriter fileWriter = new FileWriter(filename, false);
		PrintWriter out = new PrintWriter(fileWriter, true);

		String line = "";
		
		if (titles) {
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
