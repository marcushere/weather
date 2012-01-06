package org.marcus.old;

import java.io.FileWriter;
import java.io.PrintWriter;

public class DailyRecord {

	DailyPredTable overallForecastOneDay;
	HourlyPredTable hourlyForecastOneDay;
	DailyPredTable overallForecastThreeDay;
	HourlyPredTable hourlyForecastThreeDay;

	DailyActualTable actualOverall;
	HourlyActualTable actualHourly;

	String date;

	public DailyRecord(String date, DailyPredTable oneDayOverall,
			HourlyPredTable oneDayHourly, DailyPredTable threeDayOverall,
			HourlyPredTable threeDayHourly, DailyActualTable actualOverall,
			HourlyActualTable actualHourly) {
		this.date = date;
		overallForecastOneDay = oneDayOverall;
		hourlyForecastOneDay = oneDayHourly;
		overallForecastThreeDay = threeDayOverall;
		hourlyForecastThreeDay = threeDayHourly;
		this.actualOverall = actualOverall;
		this.actualHourly = actualHourly;
	}

	public void printDailyRecord(String filename, boolean append)
			throws Exception {
		FileWriter fw = new FileWriter(filename, append);
		PrintWriter pw = new PrintWriter(fw, true);
		pw.println("Daily record " + date + ":");
		pw.println(overallForecastOneDay.toString());
		pw.println(hourlyForecastOneDay.toString());
		pw.println(overallForecastThreeDay.toString());
		pw.println(hourlyForecastThreeDay.toString());
		pw.println(actualOverall.toString());
		pw.println(actualHourly.toString());
		pw.close();
	}
}
