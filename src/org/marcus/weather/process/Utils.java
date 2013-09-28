package org.marcus.weather.process;

import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public final class Utils {

	public static void printResultSet(String fileName, String query,
			String errorMessage, boolean forGNUPlot)
			throws ClassNotFoundException {
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		String connectionURL = "jdbc:sqlserver://MARCUSHANPC\\SQLEXPRESS;integratedSecurity=true;databaseName=weather;";
		try (Connection con = DriverManager.getConnection(connectionURL);
				Statement stmt = con.createStatement()) {
			try (FileWriter fw = new FileWriter(fileName);
					ResultSet rs = stmt.executeQuery(query)) {
				if (!forGNUPlot) {
					ResultSetMetaData metaData = rs.getMetaData();
					int columns = metaData.getColumnCount();
					for (int i = 1; i <= columns; i++) {
						fw.write(metaData.getColumnLabel(i));
						if (i < columns && forGNUPlot)
							fw.write(",");
					}
					fw.write("\n");
					while (rs.next()) {
						for (int i = 1; i <= columns; i++) {
							fw.write(rs.getString(i));
							if (i < columns && forGNUPlot) {
								fw.write("\t");
							} else if (i < columns && !forGNUPlot) {
								fw.write(",");
							}
						}
						fw.write("\n");
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(errorMessage);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(errorMessage);
		}
	}

	public static void printResultSet(String fileName, ResultSet rs,
			String errorMessage, boolean forGNUPlot)
			throws ClassNotFoundException, SQLException {
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		String connectionURL = "jdbc:sqlserver://MARCUSHANPC\\SQLEXPRESS;integratedSecurity=true;databaseName=weather;";
		try (Connection con = DriverManager.getConnection(connectionURL);
				Statement stmt = con.createStatement()) {
			try (FileWriter fw = new FileWriter(fileName)) {
				ResultSetMetaData metaData = rs.getMetaData();
				int columns = metaData.getColumnCount();
				fw.write("#");
				for (int i = 1; i <= columns; i++) {
					fw.write(metaData.getColumnLabel(i));
					if (i < columns)
						fw.write(",");
				}
				fw.write("\n");
				while (rs.next()) {
					for (int i = 1; i <= columns; i++) {
						final String val = rs.getString(i);
						fw.write(val);
						if (i < columns && forGNUPlot) {
							fw.write("\t");
						} else if (i < columns && !forGNUPlot) {
							fw.write(",");
						}
					}
					fw.write("\n");
				}
				fw.close();
			} catch (NullPointerException e) {
				rs.close();
				e.printStackTrace();
				System.out.println(errorMessage);
			}
		} catch (Exception e) {
			rs.close();
			e.printStackTrace();
			System.out.println(errorMessage);
		}

		rs.close();
	}
}
