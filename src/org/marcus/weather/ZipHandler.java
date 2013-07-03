package org.marcus.weather;

import java.util.Iterator;
import java.util.Map.Entry;

public class ZipHandler implements Runnable {

	DataFetcher df = null;
	DBStore db = null;
	Iterator<Entry<String, Integer>> iter = null;
	private Entry<String, Integer> currentZipEntry;
	final int id;
	private final WeatherRecorder wr;
	private final Config config;

	public ZipHandler(DBStore db, Iterator<Entry<String, Integer>> iter,
			int id, WeatherRecorder wr, Config config) throws Exception {
		df = new DataFetcher(false);
		this.db = db;
		this.iter = iter;
		this.id = id;
		this.wr = wr;
		this.config = config;
	}

	@Override
	public void run() {
		for (;;) {
			synchronized (iter) {
				if (iter.hasNext()) {
					currentZipEntry = iter.next();
				} else {
					wr.getWw().threadOutMessage("Finished", id, 4);
					return;
				}
			}
			// Report what's happening
			if (!config.isForceRun()) {
				wr.getWw().threadOutMessage(" " + currentZipEntry, id, 4);
			} else {
				wr.getWw().threadOutMessage(currentZipEntry + " (forced)", id,
						4);
			}
			// If already run, loop back and find a new zip
			if (1 != currentZipEntry.getValue() || config.isForceRun()) {
				// Load data
				df.load(currentZipEntry.getKey());

				// Write data
				try {
					synchronized (db) {
						if (df.valid) {
							db.storeForecast(df.forecast1);
							db.storeForecast(df.forecast3);
							db.storePast(df.past);
							db.updateZipSuccess(currentZipEntry.getKey());

						} else {
							// Update a failure to load data
							db.updateZipFail(currentZipEntry.getKey());
						}

						// Commit
						db.commit();
					}
				} catch (Exception e) {
					wr.setFail();
					wr.getWw().threadOutMessage(currentZipEntry + " failed",
							id, 2);
				}
				if (Thread.interrupted() || config.isStopProgram()) {
					break;
				}
			}
		}
		wr.getWw().threadOutMessage("Finished", id, 3);
	}

	public synchronized boolean isAlive() {
		return Thread.currentThread().isAlive();
	}

	public synchronized String getCurrentZip() {
		return currentZipEntry.getKey();
	}

}
