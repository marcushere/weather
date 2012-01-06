package org.marcus.weather;

public class OverallPast {

	public Float high;
	public Float precip;
	
	public OverallPast(Float dhigh, Float dprecip) {
		high = dhigh;
		precip = dprecip;
	}

	public OverallPast() {
		this.high = null;
		this.precip = null;
	}
	
}
