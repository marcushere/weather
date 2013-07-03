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
	private final WeatherRecorder wr;
	CSVStore csv = null;

	public ZipHandler(DBStore db, Iterator<Entry<String, Integer>> iter, int id, WeatherRecorder wr)
			throws Exception {
		df = new DataFetcher(false);
		this.db = db;
		this.iter = iter;
		this.id = id;
		this.wr = wr;
	}

	public ZipHandler(CSVStore csv, Iterator<Entry<String, Integer>> iter,
			int id, WeatherRecorder wr) throws Exception {
		df = new DataFetcher(false);
		this.csv = csv;
		this.iter = iter;
		this.id = id;
		this.wr = wr;
	}

	@Override
	public void run() {
		wr.getWw().threadInit(id);
		while (true) {
			while (true) {
				synchronized (iter) {
					if (iter.hasNext()) {
						currentZipEntry = iter.next();
							if (!wr.getWw().isForceRun()) {
								System.out.println("Thread " + id + " "
										+ currentZipEntry);
							} else {
								wr.getWw().threadOutMessage(currentZipEntry + " (forced)", id, 4);
						}
						if (1 != currentZipEntry.getValue()
								|| wr.getWw().isForceRun()) {
							break;
						}
					} else {
						wr.getWw().threadOutMessage("Finished", id, 4);
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
				wr.setFail();
				wr.getWw().threadOutMessage(currentZipEntry+" failed", id, 2);
			}
			if (Thread.interrupted()) {
				break;
			}
		}
		wr.getWw().threadOutMessage("Finished", id, 3);
		wr.getWw().threadFinished(id);
	}

	public synchronized boolean isAlive() {
		return Thread.currentThread().isAlive();
	}

	public synchronized String getCurrentZip() {
		return currentZipEntry.getKey();
	}

}
