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

public class ExportTableAsCSV {

	static String query = "select * from weather.dbo.daily_actual";
	static String filename = "daily_actual.csv";
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

		while (rs.next()) {
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {
				try {
					if (i == 1) {
						line = rs.getString(i);
					} else if (rs.getString(i).equals("null")){
						line = line + ",";
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
