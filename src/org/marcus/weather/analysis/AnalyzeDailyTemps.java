package org.marcus.weather.analysis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.marcus.weather.process.Utils;

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
	public static void main(String[] args) throws ClassNotFoundException,
			SQLException {
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		String connectionURL = "jdbc:sqlserver://MARCUSHANPC\\SQLEXPRESS;integratedSecurity=true;databaseName=weather;";
		Connection con = DriverManager.getConnection(connectionURL);
		Statement stmt = con.createStatement();
		final String date = new SimpleDateFormat("yyyyMMdd").format(Calendar
				.getInstance().getTime());

		// // Determine average error
		// double aveErrorOneDay = 0.0;
		// double aveErrorTwoDay = 0.0;
		// try (ResultSet rs = stmt.executeQuery(tempValsDaily)) {
		// int oneDayCount = 0;
		// int twoDayCount = 0;
		// while (rs.next()) {
		// if (rs.getDate(2).getTime() == (rs.getDate(3).getTime() + 24 * 3600 *
		// 1000)) {
		// aveErrorOneDay = aveErrorOneDay + rs.getInt(4);
		// oneDayCount++;
		// } else {
		// aveErrorTwoDay = aveErrorTwoDay + rs.getInt(4);
		// twoDayCount++;
		// }
		// }
		// System.out.println("Sum of one and three day high error.");
		// System.out.println(aveErrorOneDay);
		// System.out.println(aveErrorTwoDay);
		// aveErrorOneDay = aveErrorOneDay / oneDayCount;
		// aveErrorTwoDay = aveErrorTwoDay / twoDayCount;
		// System.out
		// .println("Average error of one and three day high forecast.");
		// System.out.println(aveErrorOneDay);
		// System.out.println(aveErrorTwoDay);
		// } catch (Exception e) {
		// System.out.println("Average error calulation failed.");
		// }
		//
		// // Determine standard deviation of error
		// try (ResultSet rs = stmt.executeQuery(tempValsDaily)) {
		// double stDevErrorOneDay = 0.0;
		// double stDevErrorTwoDay = 0.0;
		// int oneDayCount = 0;
		// int twoDayCount = 0;
		// while (rs.next()) {
		// if (rs.getDate(2).getTime() == (rs.getDate(3).getTime() + 24 * 3600 *
		// 1000)) {
		// stDevErrorOneDay = stDevErrorOneDay
		// + Math.pow(rs.getInt(4) - aveErrorOneDay, 2.0);
		// oneDayCount++;
		// } else {
		// stDevErrorTwoDay = stDevErrorTwoDay
		// + Math.pow(rs.getInt(4) - aveErrorTwoDay, 2.0);
		// twoDayCount++;
		// }
		// }
		// stDevErrorOneDay = Math.sqrt(stDevErrorOneDay / oneDayCount);
		// stDevErrorTwoDay = Math.sqrt(stDevErrorTwoDay / twoDayCount);
		// System.out
		// .println("Standard deviation of one and three day high forecast");
		// System.out.println(stDevErrorOneDay);
		// System.out.println(stDevErrorTwoDay);
		// } catch (Exception e) {
		// System.out.println("Average error calulation failed.");
		// }
		//
		// // Determine error correlation with delta_high
		// Utils.printResultSet(
		// "C:\\Users\\Marcus\\Documents\\Dropbox\\weather\\analysis\\data\\highDHigh1D"
		// + date + ".dat",
		// diffVDHigh1D,
		// "Error correlation with delta high failed. (one day both precips)",
		// true);
		//
		// Utils.printResultSet(
		// "C:\\Users\\Marcus\\Documents\\Dropbox\\weather\\analysis\\data\\highDHigh3D"
		// + date + ".dat",
		// diffVDHigh3D,
		// "Error correlation with delta high failed. (three day both precips)",
		// true);
		//
		// // Determine error correlation with precipitation
		// Utils.printResultSet(
		// "C:\\Users\\Marcus\\Documents\\Dropbox\\weather\\analysis\\data\\highDHighNP1D"
		// + date + ".dat",
		// diffVDHighNP1D,
		// "Error correlation with delta high failed. (one day no precip)",
		// true);
		// Utils.printResultSet(
		// "C:\\Users\\Marcus\\Documents\\Dropbox\\weather\\analysis\\data\\highDHighP1D"
		// + date + ".dat",
		// diffVDHighP1D,
		// "Error correlation with delta high failed. (one day with precip)",
		// true);
		// Utils.printResultSet(
		// "C:\\Users\\Marcus\\Documents\\Dropbox\\weather\\analysis\\data\\highDHighNP3D"
		// + date + ".dat",
		// diffVDHighNP3D,
		// "Error correlation with delta high failed. (three day no precip)",
		// true);
		// Utils.printResultSet(
		// "C:\\Users\\Marcus\\Documents\\Dropbox\\weather\\analysis\\data\\highDHighP3D"
		// + date + ".dat",
		// diffVDHighP3D,
		// "Error correlation with delta high failed. (three day with precip)",
		// true);

		// Determine error correlation with degrees changed from the previous
		// day

		// Get an array of the high_error values
		int[] higherrors = null;
		try (ResultSet rs1 = stmt.executeQuery(distinctHighCount)) {
			rs1.next();
			higherrors = new int[rs1.getInt(1)];
			try (ResultSet rs2 = stmt.executeQuery(distinctHighChange)) {
				for (int i = 0; i < higherrors.length; i++) {
					rs2.next();
					higherrors[i] = rs2.getInt(1);
				}
			} catch (Exception e) {
				System.out.println("Finding high changes failed");
			}
		} catch (Exception e) {
			System.out.println("Finding high changes failed");
		}

		// Make histograms of each high_error value
		// TODO: only make a histogram if distinct high count>6, otherwise find
		// the statistics manually

//		JFrame f = new JFrame();
//		f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
//		// Make the plot
//		JavaPlot plot = new JavaPlot();
//		plot.getDebugger().setLevel(Debug.VERBOSE);
//		SVGTerminal svg = new SVGTerminal();
//		plot.setTerminal(svg);
//		plot.setPersist(true);
////		plot.addPlot("'" + filename + "'" + " using 1:xticlabels(2)",
////				"title");
//		plot.newFitGraph();
		for (int i = 0; i < higherrors.length; i++) {
			try (PreparedStatement ps = con
					.prepareStatement(highErrorHistogram);
					ResultSet distHighChRS = stmt
							.executeQuery(distinctHighChange);
					PreparedStatement columnCountPS = con
							.prepareStatement(columnsInHistogram)) {
				columnCountPS.setInt(1, higherrors[i]);
				ResultSet columnCountRS = columnCountPS.executeQuery();
				columnCountRS.next();
				if(columnCountRS.getInt(1)<6){
					// TODO: find the standard deviation and mean of the data.
//					continue;
				}
				columnCountRS.close();
				final String filename = "C:/Users/Marcus/Documents/Dropbox/weather/analysis/temp/histogram"
						+ higherrors[i] + "-" + date + ".dat";
				ps.setInt(1, higherrors[i]);
//				ps.setInt(1, 0);
				Utils.printResultSet(filename, ps.executeQuery(),
						"Histogram output for high_error=" + higherrors[i]
								+ " failed.", true);
//				@SuppressWarnings("unused")
//				FileDataSet dataset = new FileDataSet(new File(filename));
//				plot.addFit(dataset, "a*exp(-(x-m)**2/(2*s**2))", "x,a,m,s".split(","), 1, "f"+Integer.toString(higherrors[i]).replace("-","n")+"i");
				

//				try {
//					Component comp = f.getContentPane().add(svg.getPanel());
//					f.pack();
//					f.setLocationRelativeTo(null);
//					f.setVisible(true);
//					System.in.read();
//					f.setVisible(false);
//
//					f.remove(comp);
//				} catch (ClassNotFoundException ex) {
//					System.err
//							.println("Error: Library SVGSalamander not properly installed?");
//				}
			} catch (Exception e) {
				System.out.println("Histogram output for high_error="
						+ higherrors[i] + " failed.");
				e.printStackTrace();
			}
		}
//		/* Do the actual fitting */
//		plot.plot();
//		/* Get and print out the fit results */
//		final HashMap<String, FitResults> collectedFitResults = plot.getCollectedFitResults();
//		printCollectedFitResults(collectedFitResults);

		System.out.println("finished");
	}

	/**
	 * @param collectedFitResults
	 */
//	private static void printCollectedFitResults(
//			final HashMap<String, FitResults> collectedFitResults) {
//		for (String key : collectedFitResults.keySet()){
//			System.out.println(key+":");
//			for (String innerKey : collectedFitResults.get(key).keySet()) {
//				System.out.println("  "+innerKey+":");
//				System.out.println("     "+collectedFitResults.get(key).get(innerKey)[0]+"   "+collectedFitResults.get(key).get(innerKey)[1]);
//			}
//		}
//	}
}
