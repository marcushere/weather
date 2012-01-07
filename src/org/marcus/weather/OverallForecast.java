package org.marcus.weather;

public class OverallForecast {

	public Integer high;
	public Integer PoP;
	
	public OverallForecast(Integer dhigh, Integer dPoP){
		high = dhigh;
		PoP = dPoP;
	}

	public OverallForecast() {
		high = null;
		PoP = null;
	}
}
