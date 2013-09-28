package org.marcus.weather.process;

import org.marcus.weather.WeatherTerm;

public class UpdateDB {

	private final WeatherTerm wt;
	private boolean failed = false;

	public UpdateDB(WeatherTerm weatherTerm) {
		wt = weatherTerm;
	}

	public void run() {
		try {
			DetermineDeltaHigh ddh = new DetermineDeltaHigh(wt, 1);
			DeterminePrecip dp = new DeterminePrecip(wt, 2);

			Thread ddhThread = new Thread(ddh);
			Thread dpThread = new Thread(dp);
			ddhThread.start();
			dpThread.start();
			
			while (ddhThread.isAlive()||dpThread.isAlive()){
				Thread.sleep(1000);
			}
			
			wt.finish();
		} catch (Exception e) {
			failed = true;
		}
	}

	public boolean isFailed() {
		return failed;
	}
}
