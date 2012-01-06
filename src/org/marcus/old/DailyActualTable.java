package org.marcus.old;

import java.sql.SQLException;
import java.util.Date;

public class DailyActualTable {

	Float high;
	Float precip;
	String occurredDate;
	String todayDate;
	String zip;

	public DailyActualTable(Float[] overallPast, String today, String occDate,
			String zipCode) {
		high = overallPast[0];
		precip = overallPast[1];
		occurredDate = occDate;
		todayDate = today;
		zip = zipCode;
	}

	public String toString() {
		String ret = "";
		String predString = "";
		predString = high + "; " + precip + "; ";
		ret = "dailyActual; " + todayDate + "; " + occurredDate + "; "
				+ predString;
		return ret;
	}

	/*
	 * A constructor using the line of data from the record file
	 */

	public DailyActualTable(String line, String zipCode) throws Exception {
		String[] split = line.split("; ");
		if (!split[0].equals("dailyActual")) {
			throw new Exception();
		}
		todayDate = split[1];
		occurredDate = split[2];
		high = Float.valueOf(split[3]);
		precip = Float.valueOf(split[4]);
		zip = zipCode;
	}

	public void updateDB(Date timestamp, DBStore db) throws ClassNotFoundException,
			SQLException {
		db.storeDA(zip, timestamp, todayDate, occurredDate, high, precip);
	}
}
