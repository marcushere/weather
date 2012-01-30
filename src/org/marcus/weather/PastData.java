package org.marcus.weather;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class PastData {

	public OverallPast overallPast;
	public HourlyPast[] hourlyPast;
	
	public Date date;
	
	public String today;
	public String occurredDate;
	
	public String zip;
	
	public PastData (String rzip, String pDate, OverallPast op, HourlyPast[] hp){
		zip = rzip;
		Calendar cal = Calendar.getInstance();
		date = cal.getTime();
		today = (new SimpleDateFormat("yyyy-MM-dd")).format(date);
		occurredDate = pDate;
		overallPast = op;
		hourlyPast = hp;
	}
	
}
