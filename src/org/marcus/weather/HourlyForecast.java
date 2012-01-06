package org.marcus.weather;

public class HourlyForecast {

	public int hour;
	public Float temp;
	public Integer PoP;
	
	public HourlyForecast(int time, Float htemp, Integer hPoP){
		hour = time;
		temp = htemp;
		PoP = hPoP;
	}
}
