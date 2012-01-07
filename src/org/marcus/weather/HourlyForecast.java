package org.marcus.weather;

public class HourlyForecast {

	public int hour;
	public Integer temp;
	public Integer PoP;
	
	public HourlyForecast(int time, Integer htemp, Integer hPoP){
		hour = time;
		temp = htemp;
		PoP = hPoP;
	}
}
