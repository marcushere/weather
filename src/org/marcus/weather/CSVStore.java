package org.marcus.weather;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;

public class CSVStore {

	FileWriter DAfileWriter;
	FileWriter DFfileWriter;
	FileWriter HAfileWriter;
	FileWriter HFfileWriter;

	PrintWriter HFout;
	PrintWriter HAout;
	PrintWriter DFout;
	PrintWriter DAout;

	SimpleDateFormat timeFormat;

	private static final String DA_NAME = "DailyActual.csv";
	private static final String DF_NAME = "DailyForecast.csv";
	private static final String HA_NAME = "HourlyActual.csv";
	private static final String HF_NAME = "HourlyForecast.csv";

	public CSVStore() throws IOException {
		DAfileWriter = new FileWriter(DA_NAME, true);
		DFfileWriter = new FileWriter(DF_NAME, true);
		HAfileWriter = new FileWriter(HA_NAME, true);
		HFfileWriter = new FileWriter(HF_NAME, true);
		DAout = new PrintWriter(DAfileWriter, true);
		DFout = new PrintWriter(DFfileWriter, true);
		HAout = new PrintWriter(HAfileWriter, true);
		HFout = new PrintWriter(HFfileWriter, true);
		timeFormat = new SimpleDateFormat("kk:mm:ss");
	}

	public void storeForecast(ForecastData forecast) {
		String prefix = forecast.zip + "," + timeFormat.format(forecast.date)
				+ "," + forecast.today + "," + forecast.forecastDate;
		if (forecast.overallForecast.high != null
				|| forecast.overallForecast.PoP != null) {
			String str = prefix;
			str = str + ",";
			if (forecast.overallForecast.high != null)
				str = str + forecast.overallForecast.high.toString();
			str = str + ",";
			if (forecast.overallForecast.PoP != null)
				str = str + forecast.overallForecast.PoP.toString();
			DFout.println(str);
		}

		if (forecast.hourlyForecast.length != 0) {
			for (int i = 0; i < forecast.hourlyForecast.length; i++) {
				String str = prefix;
				str = str + ",";
				str = str + forecast.hourlyForecast[i].hour;
				str = str + ",";
				if (forecast.hourlyForecast[i].temp != null)
					str = str + forecast.hourlyForecast[i].temp;
				str = str + ",";
				if (forecast.hourlyForecast[i].PoP != null)
					str = str + forecast.hourlyForecast[i].PoP;
				HFout.println(str);
			}
		}
	}

	public void storePast(PastData past) {
		String prefix = past.zip + "," + timeFormat.format(past.date) + ","
				+ past.today + "," + past.forecastDate;
		if (past.overallPast.high != null || past.overallPast.precip != null) {
			String str = prefix;
			str = str + ",";
			if (past.overallPast.high != null)
				str = str + past.overallPast.high.toString();
			str = str + ",";
			if (past.overallPast.precip != null)
				str = str + past.overallPast.precip.toString();
			DAout.println(str);
		}

		if (past.hourlyPast.length != 0) {
			for (int i = 0; i < past.hourlyPast.length; i++) {
				String str = prefix;
				str = str + "," + past.hourlyPast[i].hour;
				str = str + ",";
				if (past.hourlyPast[i].temp != null)
					str = str + past.hourlyPast[i].temp.toString();
				str = str + ",";
				if (past.hourlyPast[i].conditions != null)
					str = str + past.hourlyPast[i].conditions;
				str = str + ",";
				if (past.hourlyPast[i].precip != null)
					str = str + past.hourlyPast[i].precip.toString();
				str = str + ",";
				HAout.println(str);
			}
		}
	}
}
