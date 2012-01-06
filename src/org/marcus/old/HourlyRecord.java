package org.marcus.old;

public class HourlyRecord {
	
	int time;
	Float temp;
	Float precip;
	String conditions;
	
	public HourlyRecord (int pastHours, Float hTemp, Float hPrecip, String hCond){
		time = pastHours;
		temp = hTemp;
		precip = hPrecip;
		conditions = hCond;
	}

	public HourlyRecord(String hTime, String hTemp, String hCond) {
		time = Integer.parseInt(hTime);
		temp = Float.valueOf(hTemp);
		conditions = hCond;
	}

	public boolean hasPrecip (){
		return precip != null;
	}
}
