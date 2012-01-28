package org.marcus.weather;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class WeatherRecorder {

	private static final String LOG_NAME = "log.txt";
	private static final String ZIPS_FILE = "zips.txt";
	private static final String ERROR_NAME = "error.txt";

	public static void main(String[] args) {
		CheckKeyboard ck = new CheckKeyboard();
		Thread thread = new Thread(ck);
		thread.start();
		boolean useDB = true;
		if (args.length > 0) {
			for (int i = 0; i < args.length; i++) {
				if (args[i].equals("-csv")) {
					useDB = false;
				}
			}
		}
		try {
			String[] zips;
			try {
				if (hasRunToday())
					return;
				zips = getZips();
			} catch (IOException e) {
				return;
			}
			int i = 0;

			DBStore db = null;
			if (useDB)
				db = new DBStore();
			CSVStore csv = null;
			if (!useDB)
				csv = new CSVStore();

			try {
				db.open();
			} catch (Exception e) {
				FileWriter fileWriter;
				fileWriter = new FileWriter(LOG_NAME, false);
				PrintWriter out = new PrintWriter(fileWriter, true);
				SimpleDateFormat format = getDateFormatter();
				out.print("error " + format.format(new Date()));

				out.close();
				printError(e, zips[i]);
				return;
			}
			try {
				for (i = 0; i < zips.length && !ck.isStop(); i++) {
					System.out.println(zips[i]);
					DataFetcher df = new DataFetcher(zips[i]);
					if (df.valid && useDB) {
						db.storeForecast(df.forecast1);
						db.storeForecast(df.forecast3);
						db.storePast(df.past);
						db.commit();
					} else if (df.valid && !useDB) {
						csv.storeForecast(df.forecast1);
						csv.storeForecast(df.forecast3);
						csv.storePast(df.past);
					}
				}
				db.close();
				FileWriter fileWriter = new FileWriter(LOG_NAME, false);
				PrintWriter out = new PrintWriter(fileWriter, true);
				out.print("ok " + getYMDFormatter().format(new Date()));
				out.close();
				fileWriter.close();
			} catch (Exception e) {
				FileWriter fileWriter;
				try {
					db.commit();
					db.close();
					fileWriter = new FileWriter(LOG_NAME, false);
					PrintWriter out = new PrintWriter(fileWriter, true);

					SimpleDateFormat format = getYMDFormatter();

					out.print("error " + format.format(new Date()) + " "
							+ zips[i]);

					out.close();
					fileWriter.close();
					printError(e, zips[i]);
				} catch (IOException e1) {
					e1.printStackTrace();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static boolean hasRunToday() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(LOG_NAME));
		String line = br.readLine();
		if (line == null) {
			return false;
		}
		int hour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
		String[] pieces = line.split(" ");
		if (pieces.length == 2 && pieces[0].equals("ok")) {
			String now = getYMDFormatter().format(new Date());
			if (pieces[1].equals(now)) {
				return true;
			} else if (hour > 4) {
				return false;
			}
		}
		return false;
	}

	private static String[] getZips() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(LOG_NAME));
		String line = br.readLine();
		String[] splitted = line.split(" ");
		String lastZip = "";
		if (splitted.length > 2)
			lastZip = splitted[2];
		if (splitted[1].equals(getYMDFormatter().format(new Date())))
			lastZip = "";

		boolean pastLastZip = false;
		br = new BufferedReader(new FileReader(ZIPS_FILE));
		line = "";
		String read = br.readLine();
		while (read != null) {
			if (lastZip.isEmpty()) {
				if (line.isEmpty()) {
					line = read;
				} else {
					line = line + " " + read;
				}
			} else if (!lastZip.isEmpty() && pastLastZip) {
				if (line.isEmpty()) {
					line = read;
				} else {
					line = line + " " + read;
				}
			} else if (!lastZip.isEmpty() && !pastLastZip) {
				if (line.contains(lastZip))
					pastLastZip = true;
			}
			read = br.readLine();
		}
		return line.split(" ");
	}

	private static SimpleDateFormat getYMDFormatter() {
		return new SimpleDateFormat("yyyy-MM-dd");
	}

	private static SimpleDateFormat getDateFormatter() {
		return new SimpleDateFormat("yyyy-MM-dd'T'kk:mm:ss.SSS");
	}

	private static void printError(Exception e, String zip) throws IOException {
		FileWriter fileWriter;
		PrintWriter out;
		fileWriter = new FileWriter(ERROR_NAME, true);
		out = new PrintWriter(fileWriter, true);

		out.print("error " + getDateFormatter().format(new Date()) + " " + zip);
		out.println();
		e.printStackTrace(out);
		out.print(e.getMessage());
		out.println();
		out.close();
		fileWriter.close();
	}

}
