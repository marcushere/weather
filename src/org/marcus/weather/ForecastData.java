package org.marcus.weather;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class ForecastData {
	
	public OverallForecast overallForecast;
	public HourlyForecast[] hourlyForecast;
	
	public Date date;
	
	public String today;
	public String forecastDate;
	
	public String zip;
	
	public ForecastData(String rzip, String fDate, OverallForecast df, HourlyForecast[] hf){
		zip = rzip;
		Calendar cal = Calendar.getInstance();
		date = cal.getTime();
		today = (new SimpleDateFormat("yyyy-MM-dd")).format(date);
		forecastDate = fDate;
		overallForecast = df;
		hourlyForecast = hf;
	}
	
}
