package org.marcus.weather;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class CheckKeyboard implements Runnable {

	private volatile boolean stopProgram = false;

	public synchronized boolean isStopProgram() {
		return stopProgram;
	}

	private synchronized void setStopProgram(boolean stop) {
		this.stopProgram = stop;
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
		if (line.equalsIgnoreCase("c"))
			setStopProgram(false);
	}

}
