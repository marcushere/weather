package org.marcus.weather;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.marcus.old.DBStore;
import org.marcus.old.DailyActualTable;
import org.marcus.old.DataFetcher;
import org.marcus.old.HourlyActualTable;

public class OldDataFetcher {

	private static final int DAY_IN_MILLIS = 1000 * 3600 * 24;
	private static String[] zips;
	private static String date = "2011-08-19";
	private static String today;

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		new OldDataFetcher();
	}

	public OldDataFetcher() throws Exception {
		today = getYMDFormatter().format((Calendar.getInstance()).getTime());
		zips = scanZips("zips.txt");
		DBStore db = new DBStore();
		db.open();
		for (int i = 0; i < zips.length; i++) {
			findAndRecord(zips[i], date, db);
			System.out.println(i);
		}
		db.close();
	}

	private static void findAndRecord(String zip, String date, DBStore db)
			throws Exception {
		HourlyActualTable tempHAT;
		DailyActualTable tempDAT;
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(cal.getTimeInMillis() - DAY_IN_MILLIS);
		int i = 1;
		SimpleDateFormat format = getYMDFormatter();
		FileWriter fileWriter = new FileWriter("data\\new" + zip + ".dat", true);
		PrintWriter out = new PrintWriter(fileWriter, true);
		while (!format.format(cal.getTime()).equalsIgnoreCase(date)) {
			Date timestamp = Calendar.getInstance().getTime();
			DataFetcher df = new DataFetcher(zip, cal.getTime());
			tempHAT = makeNewHAT(df);
			out.println(tempHAT);
			tempHAT.updateDB(timestamp, db);
			tempDAT = makeNewDAT(df);
			out.println(tempDAT);
			tempDAT.updateDB(timestamp, db);
			cal.setTimeInMillis(cal.getTimeInMillis() - DAY_IN_MILLIS);
			System.out.println("zip:" + zip + "." + i + " "
					+ format.format(cal.getTime()));
			db.commit();
			i++;
		}
	}

	private static DailyActualTable makeNewDAT(DataFetcher df) {
		return new DailyActualTable(df.getOverallWPast(), today,
				getYMDFormatter().format(df.pastDate), df.zip);
	}

	private static HourlyActualTable makeNewHAT(DataFetcher df) {
		return new HourlyActualTable(df.getHourlyPastWTemp(),
				df.getHourlyPastWPrecip(), df.getHourlyPastWCond(),
				df.getPastWTimes(), today, getYMDFormatter()
						.format(df.pastDate), df.zip);
	}

	private static SimpleDateFormat getYMDFormatter() {
		return new SimpleDateFormat("yyyy-MM-dd");
	}

	private static String[] scanZips(String filename) throws Exception {
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

}
