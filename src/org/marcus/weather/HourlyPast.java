package org.marcus.weather;

public class HourlyPast {

	public int hour;
	public Float temp;
	public Float precip;
	public String conditions;
	
	public HourlyPast(int time, Float htemp, Float hprecip, String hconditions){
		this.hour = time;
		this.temp = htemp;
		this.precip = hprecip;
		this.conditions = hconditions;
	}

	public HourlyPast(int rhour) {
		this.hour = rhour;
		this.temp = null;
		this.precip = null;
		this.conditions = "";
	}
	
}
