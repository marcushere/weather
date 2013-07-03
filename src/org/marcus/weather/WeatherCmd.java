package org.marcus.weather;

import java.io.IOException;

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
				}
			}
		}

		if (runTerminal) {
			ww = new WeatherTerm(args);
		} else {
			// ww = new WeatherGUI(args);
		}

		try {
			ww.run();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
