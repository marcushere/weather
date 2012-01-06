package org.marcus.old;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DailyRecordedInfo {

	DailyPredTable DPT;
	HourlyPredTable HPT;
	DailyPredTable DPT_3_Day;
	HourlyPredTable HPT_3_Day;

	DailyActualTable yesterdayDAT;
	HourlyActualTable yesterdayHAT;

	String date;
	String fullDate;
	String zip;

	public DailyRecordedInfo(String[] fileData, String zipCode) throws Exception {
		zip = zipCode;
		for (int i = 0; i < 7; i++) {
			if (fileData[i].charAt(0) == '#') {
				String[] split = fileData[i].split(" ");
				date = split[1].split("T")[0];
				fullDate = split[1];
				break;
			} else {
				String[] split = fileData[i].split("; ");
				if (split[0].equals("hourlyPred")) {
					if (isThreeDay(split)) {
						HPT_3_Day = new HourlyPredTable(fileData[i], zip);
					} else {
						HPT = new HourlyPredTable(fileData[i], zip);
					}
				} else if (split[0].equals("dailyPred")) {
					if (isThreeDay(split)) {
						DPT_3_Day = new DailyPredTable(fileData[i], zip);
					} else {
						DPT = new DailyPredTable(fileData[i], zip);
					}
				} else if (split[0].equals("hourlyActual")) {
					yesterdayHAT = new HourlyActualTable(fileData[i], zip);
				} else if (split[0].equals("dailyActual")) {
					yesterdayDAT = new DailyActualTable(fileData[i], zip);
				}
			}
		}
	}

	private boolean isThreeDay(String[] line) throws Exception {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Date fDate = format.parse(line[1]);
		Date pDate = format.parse(line[2]);
		Calendar fCal = Calendar.getInstance();
		Calendar pCal = Calendar.getInstance();
		fCal.setTime(fDate);
		pCal.setTime(pDate);
		if (fCal.get(Calendar.DAY_OF_YEAR) + 1 == pCal
				.get(Calendar.DAY_OF_YEAR)) {
			return false;
		} else if (fCal.get(Calendar.DAY_OF_YEAR) + 3 == pCal
				.get(Calendar.DAY_OF_YEAR)) {
			return true;
		} else if (fCal.get(Calendar.DAY_OF_YEAR) + 1 > 365) {
			if (365 - fCal.get(Calendar.DAY_OF_YEAR)
					+ pCal.get(Calendar.DAY_OF_YEAR) == 3) {
				return true;
			} else {
				return false;
			}
		} else {
			throw new Exception();
		}
	}

	public void printData(String filename, boolean append) throws Exception {
		FileWriter fw = new FileWriter(filename, append);
		PrintWriter pw = new PrintWriter(fw, true);
		pw.println(DPT.toString());
		pw.println(HPT.toString());
		pw.println(DPT_3_Day.toString());
		pw.println(HPT_3_Day.toString());
		pw.println(yesterdayDAT.toString());
		pw.println(yesterdayHAT.toString());
		pw.println("#finished " + fullDate);
		pw.close();
	}

}
