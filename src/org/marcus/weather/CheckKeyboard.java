package org.marcus.weather;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class CheckKeyboard implements Runnable {
	
	private final WeatherTerm wt;

	public CheckKeyboard(WeatherTerm weatherTerm) {
		wt = weatherTerm;
	}

	@Override
	public void run() {
		InputStreamReader isr = new InputStreamReader(System.in);
		BufferedReader br = new BufferedReader(isr);
		String line = null;
		try {
			line = br.readLine();
		} catch (IOException e) {
		}
		if (line.equalsIgnoreCase("c")){
			wt.stopProgram();
			wt.mainOutMessage("quitting...",0);
		}
	}

}
