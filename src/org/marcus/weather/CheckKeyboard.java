package org.marcus.weather;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CheckKeyboard implements Runnable {
	
	private List<Thread> threadList = new ArrayList<Thread>();

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
		if (line.equalsIgnoreCase("c")){
			setStopProgram(true);
			System.out.println("quitting...");
			Iterator<Thread> iter = threadList.iterator();
			while (iter.hasNext()){
				(iter.next()).interrupt();
			}
		}
	}
	
	public void addThread(Thread t){
		threadList.add(t);
	}

}
