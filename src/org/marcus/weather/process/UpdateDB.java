package org.marcus.weather.process;

import java.io.IOException;

import org.marcus.weather.WeatherTerm;

public class UpdateDB {

	private final WeatherTerm wt;
	private boolean failed = false;

	public UpdateDB(WeatherTerm weatherTerm) {
		this.wt = weatherTerm;
	}

	public void run() throws InterruptedException, IOException {
		DetermineDeltaHigh ddh = new DetermineDeltaHigh(wt, 1);
		DeterminePrecip dp = new DeterminePrecip(wt, 2);

		try {

			wt.mainOutMessage("UpdateDB> Starting delta highs thread (1)", 3);
			Thread ddhThread = new Thread(ddh);
			wt.mainOutMessage("UpdateDB> Starting precip thread (2)", 3);
			Thread dpThread = new Thread(dp);
			
			ddhThread.start();
			dpThread.start();
			
			while (ddhThread.isAlive() || dpThread.isAlive()) {
				Thread.sleep(1000);
			}

		} catch (Exception e) {
			failed = true;
		}

		wt.finish();
	}

	public boolean isFailed() {
		return failed;
	}

}
