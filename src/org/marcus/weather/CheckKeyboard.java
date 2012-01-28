package org.marcus.weather;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class CheckKeyboard implements Runnable {

	private volatile boolean stop = false;

	public synchronized boolean isStop() {
		return stop;
	}

	private synchronized void setStop(boolean stop) {
		this.stop = stop;
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
			setStop(false);
	}

}
