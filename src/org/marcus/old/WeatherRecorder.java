package org.marcus.old;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.marcus.old.WeatherData;

public class WeatherRecorder {

	private static final String ERROR_NAME = "error.txt";
	private static final String LOG_NAME = "log.txt";

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		if (!updatedToday()) {
			try {
				DBStore db = new DBStore();
				String[] zips = scanZips("zips.txt");
				System.out.println("1");
				WeatherData[] wd = grabData(zips);
				System.out.println("2");
				db.open();
				recordFiles(wd, db);
				db.close();
				System.out.println("3");

				// WeatherData wd55123 = new WeatherData("55123");
				// WeatherData wd52101 = new WeatherData("52101");
				//
				// updateFile("55123.dat", wd55123);
				// updateFile("52101.dat", wd52101);

				SimpleDateFormat format = getYMDFormatter();

				FileWriter fileWriter;
				fileWriter = new FileWriter(LOG_NAME, false);
				PrintWriter out = new PrintWriter(fileWriter, true);
				out.print("ok " + format.format(new Date()));
				out.close();
			} catch (Exception e) {
				FileWriter fileWriter;
				try {
					fileWriter = new FileWriter(LOG_NAME, false);
					PrintWriter out = new PrintWriter(fileWriter, true);

					SimpleDateFormat format = getDateFormatter();

					out.print("error " + format.format(new Date()));

					out.close();
					printError(e, "");
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		}
	}

	public static void printError(Exception e, String zip) throws IOException {
		FileWriter fileWriter;
		PrintWriter out;
		fileWriter = new FileWriter(ERROR_NAME, true);
		out = new PrintWriter(fileWriter, true);

		out.print("error " + getYMDFormatter().format(new Date()) + " " + zip);
		out.println();
		e.printStackTrace(out);
		out.print(e.getMessage());
		out.println();
		out.close();
	}

	private static SimpleDateFormat getYMDFormatter() {
		return new SimpleDateFormat("yyyy-MM-dd");
	}

	private static SimpleDateFormat getDateFormatter() {
		return new SimpleDateFormat("yyyy-MM-dd'T'kk:mm:ss.SSS");
	}

	public static void updateFile(String filename, WeatherData wd, DBStore db)
			throws IOException, ClassNotFoundException, SQLException {
		HourlyPredTable hpt = new HourlyPredTable(wd.temps, wd.precips,
				wd.forecastTimes, wd.date, wd.forecastDate, wd.zipCode);
		DailyPredTable dpt = new DailyPredTable(wd.forecast, wd.date,
				wd.forecastDate, wd.zipCode);
		HourlyActualTable hat = new HourlyActualTable(wd.tempsPast,
				wd.precipPast, wd.conditionsPast, wd.pastTimes, wd.date,
				wd.pastDate, wd.zipCode);
		DailyActualTable dat = new DailyActualTable(wd.overallPast, wd.date,
				wd.pastDate, wd.zipCode);
		HourlyPredTable hpt3 = new HourlyPredTable(wd.threeDayTemps,
				wd.threeDayPrecips, wd.forecastTimes3, wd.date,
				wd.threeDayDate, wd.zipCode);
		DailyPredTable dpt3 = new DailyPredTable(wd.threeDayForecast, wd.date,
				wd.threeDayDate, wd.zipCode);

		FileWriter fileWriter = new FileWriter(filename, true);
		PrintWriter out = new PrintWriter(fileWriter, true);
		out.println(hpt);
		out.println(dpt);
		out.println(hat);
		out.println(dat);
		out.println(hpt3);
		out.println(dpt3);

		SimpleDateFormat format = getDateFormatter();

		Date now = new Date();
		out.println("#finished " + format.format(now));
		out.close();

		String timestamp = format.format(now).split("T")[1];
		timestamp = timestamp.substring(0, timestamp.length() - 4);

		hpt.updateDB(timestamp);
		dpt.updateDB(timestamp);
		hat.updateDB(now, db);
		dat.updateDB(now, db);
		hpt3.updateDB(timestamp);
		dpt3.updateDB(timestamp);
		db.commit();
	}

	public static boolean updatedToday() throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(LOG_NAME));
		String line = br.readLine();
		if (line == null) {
			return false;
		}
		int hour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
		String[] pieces = line.split(" ");
		if (pieces.length == 2 && pieces[0].equals("ok")) {
			SimpleDateFormat format = getYMDFormatter();
			String now = format.format(new Date());
			if (pieces[1].equals(now)) {
				return true;
			} else if (hour > 4) {
				return false;
			}
		}
		return false;
	}

	public static String[] scanZips(String filename) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(filename));
		String line = br.readLine();
		String read = br.readLine();
		while (read != null) {
			line = line + " " + read;
			read = br.readLine();
		}
		return line.split(" ");
		// return (new String("02901 02108")).split(" ");
	}

	public static WeatherData[] grabData(String[] zips) throws Exception {
		WeatherData[] wd = new WeatherData[zips.length];
		for (int i = 0; i < zips.length; i++) {
			System.out.println(zips[i]);
			wd[i] = new WeatherData(zips[i]);
		}
		return wd;
	}

	public static void recordFiles(WeatherData[] wd, DBStore db)
			throws Exception {
		for (int i = 0; i < wd.length; i++) {
			updateFile("data\\" + wd[i].zipCode + ".dat", wd[i], db);
			// updateFile("datatest\\" + wd[i].zipCode + ".dat", wd[i], db);
		}
	}
}
