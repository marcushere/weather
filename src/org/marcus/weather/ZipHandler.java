package org.marcus.weather;

import java.util.Iterator;
import java.util.Map.Entry;

public class ZipHandler implements Runnable {

	DataFetcher df = null;
	DBStore db = null;
	Iterator<Entry<String, Integer>> iter = null;
	boolean useDB = true;
	private Entry<String, Integer> currentZipEntry;
	final int id;
	private final WeatherWrapper ww;

	CSVStore csv = null;

	public ZipHandler(DBStore db, Iterator<Entry<String, Integer>> iter, int id, WeatherWrapper ww)
			throws Exception {
		df = new DataFetcher(false);
		this.db = db;
		this.iter = iter;
		this.id = id;
		this.ww = ww;
	}

	public ZipHandler(CSVStore csv, Iterator<Entry<String, Integer>> iter,
			int id, WeatherWrapper ww) throws Exception {
		df = new DataFetcher(false);
		this.csv = csv;
		this.iter = iter;
		this.id = id;
		this.ww = ww;
	}

	@Override
	public void run() {
		while (true) {
			while (true) {
				synchronized (iter) {
					if (iter.hasNext()) {
						currentZipEntry = iter.next();
						if (WeatherRecorder.debug) {
							if (!WeatherRecorder.isForceRun()) {
								System.out.println("Thread " + id + " "
										+ currentZipEntry);
							} else {
								System.out.println("Thread " + id + " "
										+ currentZipEntry + " (forced)");
							}
						}
						if (1 != currentZipEntry.getValue()
								|| WeatherRecorder.isForceRun()) {
							break;
						}
					} else {
						if (WeatherRecorder.debug)
							System.out.println(">ZipHandler.run(1/1) Thread " + id
									+ " has finished");
						return;
					}
				}
			}

			df.load(currentZipEntry.getKey());

			try {
				if (df.valid && useDB) {
					synchronized (db) {
						db.storeForecast(df.forecast1);
						db.storeForecast(df.forecast3);
						db.storePast(df.past);
						db.updateZipSuccess(currentZipEntry.getKey());
					}
				} else if (useDB) {
					synchronized (db) {
						db.updateZipFail(currentZipEntry.getKey());
					}
				} else {
					synchronized (csv) {
						csv.storeForecast(df.forecast1);
						csv.storeForecast(df.forecast3);
						csv.storePast(df.past);
					}
				}
				synchronized (db) {
					db.commit();
				}
			} catch (Exception e) {
				WeatherRecorder.setFail();
				if (WeatherRecorder.debug)
					System.out.println("Thread " + id + " " + currentZipEntry
							+ " failed");
			}
			if (Thread.interrupted()) {
				break;
			}
		}
		if (WeatherRecorder.debug)
			System.out.println(">ZipHandler.run(1/1) Thread " + id
					+ " has finished");
	}

	public synchronized boolean isAlive() {
		return Thread.currentThread().isAlive();
	}

	public synchronized String getCurrentZip() {
		return currentZipEntry.getKey();
	}

}
