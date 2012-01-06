package org.marcus.old;

import java.sql.SQLException;
import java.util.Date;

public class HourlyActualTable {

	Float[] hourlyTemp;
	Float[] hourlyPrecip;
	int[] hours;
	String[] hourlyConditions;

	String occurredDate;
	String todayDate;
	String zip;

	HourlyRecord[] records;

	public HourlyActualTable(Float[] temps, Float[] precips, String[] conds,
			int[] pastHours, String today, String occDate, String zipCode) {
		hourlyTemp = temps;
		hourlyPrecip = precips;
		hours = pastHours;
		hourlyConditions = conds;

		occurredDate = occDate;
		todayDate = today;
		zip = zipCode;

		records = new HourlyRecord[temps.length];

		for (int i = 0; i < temps.length; i++) {
			records[i] = new HourlyRecord(pastHours[i], temps[i], precips[i],
					conds[i]);
		}
	}

	public String toString() {
		String ret = "";
		String predString = "";
		for (int i = 0; i < records.length; i++) {
			predString = predString + records[i].time + "; " + records[i].temp
					+ ";  " + records[i].conditions + "; ";
		}
		ret = "hourlyActual; " + todayDate + "; " + occurredDate + "; "
				+ predString;
		return ret;
	}

	/*
	 * A constructor using the line of data from the record file
	 */

	public HourlyActualTable(String line, String zipCode) throws Exception {
		String[] split = line.split("; ");
		if (!split[0].equals("hourlyActual")) {
			throw new Exception();
		}
		todayDate = split[1];
		occurredDate = split[2];
		records = new HourlyRecord[(split.length - 3) / 3];
		int j = 0;
		for (int i = 3; i + 2 < split.length; i = i + 3) {
			records[j] = new HourlyRecord(split[i], split[i + 1], split[i + 2]);
			j++;
		}
		zip = zipCode;
	}

	public void updateDB(Date timestamp, DBStore db)
			throws ClassNotFoundException, SQLException {

		if (hours.length > 0 && hourlyTemp.length > 0
				&& hourlyPrecip.length > 0) {
			for (int i = 0; i < hourlyTemp.length; i++) {
				try {
					db.storeHA(zip, timestamp, todayDate, occurredDate,
							hours[i], hourlyTemp[i], hourlyPrecip[i],
							hourlyConditions[i]);
				} catch (Exception e) {
					System.out.println(e);
				}
			}
		}
	}
}