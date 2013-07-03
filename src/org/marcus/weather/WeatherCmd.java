package org.marcus.weather;

import java.util.Date;

public class WeatherCmd {

	private static boolean runTerminal = false;
	private static WeatherWrapper ww = null;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length > 0) {
			for (int i = 0; i < args.length; i++) {
				if (args[i].equals("--term")) {
					runTerminal = true;
				} else {
					runTerminal = false;
				}
			}
		}
		
		ww = new WeatherTerm(args);
		
	}
}
