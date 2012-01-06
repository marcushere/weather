package org.marcus.old;

import java.text.SimpleDateFormat;
import java.util.Date;

public class WeatherData {

	String[] temps;
	String[] precips;
	String[] forecast;
	
	Float[] overallPast;
	Float[] tempsPast;
	Float[] precipPast;
	String[] conditionsPast;
	
	String[] threeDayTemps;
	String[] threeDayPrecips;
	String[] threeDayForecast;
	
	String[] forecastTimes;
	String[] forecastTimes3;
	int[] pastTimes;
	
	String date;
	String forecastDate;
	String threeDayDate;
	String pastDate;
	
	String zipCode;

	public WeatherData(String zip) throws Exception {
		{
			DataFetcher dataFetcher = new DataFetcher(zip, 1);
			forecastTimes = dataFetcher.getForecastTimes();
			temps = dataFetcher.getHourlyForecastTemps();
			precips = dataFetcher.getHourlyForecastPrecip();
			forecast = dataFetcher.getOverallForecast();
			pastTimes = dataFetcher.getPastWTimes();
			overallPast = dataFetcher.getOverallWPast();
			tempsPast = dataFetcher.getHourlyPastWTemp();
			precipPast = dataFetcher.getHourlyPastWPrecip();
			conditionsPast = dataFetcher.getHourlyPastWCond();
			zipCode = zip;
		}
		
		{
			DataFetcher dataFetcher = new DataFetcher(zip, 3);
			forecastTimes3 = dataFetcher.getForecastTimes();
			threeDayTemps = dataFetcher.getHourlyForecastTemps();
			threeDayPrecips = dataFetcher.getHourlyForecastPrecip();
			threeDayForecast = dataFetcher.getOverallForecast();
		}
		
		Date now = new Date();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		
		date = format.format(now);
		forecastDate = format.format(new Date(new Date().getTime()+3600*24*1000));
		threeDayDate = format.format(new Date(new Date().getTime()+3600*24*1000*3));
		pastDate = format.format(new Date(new Date().getTime()-3600*24*1000));
	}
}
