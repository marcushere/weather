package org.marcus.weather;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class WeatherRecorder {

	private static class EarlyTerminationException extends Exception {
		private static final long serialVersionUID = 7107342102877398736L;
	}

	private static final String ZIPS_FILE = "zips.txt";

	private final WeatherWrapper ww;

	private boolean anyFails = false;

	public WeatherRecorder(WeatherWrapper ww) {
		this.ww = ww;
	}

	public void run() throws IOException {

		try {
			// create the zip map
			Map<String, Integer> zips = null;
			// populate the zip map
			try {
				if (ww.isPastOnly()) {
					ww.mainOutMessage(
							"WR.run> Retrieving all zip codes for past data aquisition...",
							4);
					zips = getZips();
				} else {
					ww.mainOutMessage(
							"WR.run> Checking for previous runs today...", 4);
					ww.mainOutMessage("WR.run> Retrieving zip codes...", 4);
					zips = getZips();
				}
			} catch (SQLException e) {
				ww.exceptionHandler(e, 2,
						"WR.run> No database connection (exit code 2)");
			} catch (Exception e) {
				ww.exceptionHandler(e, 1, "WR.run> Unknown error (exit code 1)");
			}

			ww.mainOutMessage(
					"WR.run> Zip code retrieval finished, creating storage object",
					4);

			// Check if all of the zips are already complete
			if (!zips.values().contains(0) && !ww.isForceRun()) {
				ww.mainOutMessage("WR.run> All zips completed today", 4);
				ww.finish();
			}

			// Make the DBStore object
			DBStore db = null;
			if (ww.isUseDB())
				db = new DBStore(ww.isForceRun(), !ww.isSimRun()); // tell the
																	// database
			// object whether or not
			// to overwrite existing
			// rows
			// Make the CSVStore object
			CSVStore csv = null;
			if (!ww.isUseDB())
				csv = new CSVStore();

			// Try to open the database connection twice before giving up
			for (int openDBint = 0; openDBint < 2; openDBint++) {
				ww.mainOutMessage("WR.run> Opening database connection...", 4);
				try {
					if (ww.isUseDB())
						db.open();
					break; // break out of the loop if the database connection
							// opens successfully
				} catch (Exception e) {
					ww.exceptionHandler(e, 2,
							"WR.run> No database connection (exit code 2)");
				}
			}

			ww.mainOutMessage(
					"WR.run> Storage object ready, beginning to collect and store data...",
					4);

			// Create iterator object
			Iterator<Entry<String, Integer>> iter = zips.entrySet().iterator();
			
			//Create threads object
			Thread[] zipHandlerThreads = createThreadsContainer(iter, db, csv);

			// Start collecting datas
			collectData(zipHandlerThreads, iter, db);

			// This could only be from an error message or the CSVStore object
			// initialization
		} catch (IOException e) {
			e.printStackTrace();
			ww.exceptionHandler(e, 4,
					"WR.main(unk) Passing exit code 4, IOException.");
		}

	}

	private Thread[] createThreadsContainer(Iterator<Entry<String, Integer>> iter, DBStore db, CSVStore csv) {
		// Create threads container
		Thread[] zipHandlerThreads = new Thread[ww.getNumThreads()];
		try { // Create the DataFetcher object
			ww.mainOutMessage(
					"WR.createThreadsContainer> Initializing zipHandlerThread objects...", 4);

			// Loop through the thread array
			for (int i = 0; i < zipHandlerThreads.length; i++) {
				ww.mainOutMessage("WR.createThreadsContainer> Creating thread " + (i + 1) + "/"
						+ zipHandlerThreads.length, 4);
				// Create the new threads
				if (ww.isUseDB()) {
					zipHandlerThreads[i] = new Thread(new ZipHandler(db, iter,
							i, this));
				} else {
					zipHandlerThreads[i] = new Thread(new ZipHandler(csv, iter,
							i, this));
				}
			}

			// If something went wrong with the thread creation
		} catch (Exception e) {
			try {
				if (ww.isUseDB()) {
					db.commit();
					db.close();
				}
			} catch (SQLException e1) {
				e1.printStackTrace();
			}

			ww.mainOutMessage(
					"WR.createThreadsContainer> Passing exit code 3, ZipHandler initialization failed",
					1);
		}
		return zipHandlerThreads;
	}
	
	private void collectData(Thread[] zipHandlerThreads, Iterator<Entry<String, Integer>> iter, DBStore db) throws IOException{
		try {
			for (int i = 0; i < zipHandlerThreads.length; i++) {
				// Start each of the threads
				ww.mainOutMessage("WR.collectData> Starting thread " + (i+1) + "/"
						+ zipHandlerThreads.length, 4);
				zipHandlerThreads[i].start();
			}

			boolean stillRunning = true;

			while (stillRunning) {
				stillRunning = false;
				// Check to make sure at least one thread is still alive
				for (int i = 0; i < zipHandlerThreads.length; i++) {
					if (zipHandlerThreads[i].isAlive()) {
						stillRunning = true;
						zipHandlerThreads[i]
								.join(4000 / zipHandlerThreads.length);
					}
				}

				// Check if user requested early termination
				if (ww.isStopProgram()) {
					// Stop each of the threads
					for (int i = 0; i < zipHandlerThreads.length; i++) {
						zipHandlerThreads[i].interrupt();
					}
					// Set the rest of the zips as failing in the database
					if (ww.isUseDB()) {
						synchronized (iter) {
							while (iter.hasNext()) {
								String currentZip = iter.next().getKey();
								synchronized (db) {
									db.updateZipFail(currentZip);
								}
								anyFails = true;
							}
						}
					}
					// EarlyTerminationException, caught below
					throw new EarlyTerminationException();
				}
				Thread.sleep(1000);
			}

			// Data gathering has finished, give messages next
			if (anyFails) {
				ww.mainOutMessage(
						"WR.collectData> Data retrieval and storage parially successful",
						3);
			} else if (!anyFails) {
				ww.mainOutMessage(
						"WR.collectData> Data retrieval and storage successful", 3);
			}

			if (ww.isUseDB())
				ww.mainOutMessage("WR.collectData> Closing database connection", 4);
			if (ww.isUseDB())
				db.close();

			ww.finish();

			// Catching exceptions thrown during data collection or storage
		} catch (Exception e) {
			SimpleDateFormat format = getYMDFormatter();
			try {
				if (ww.isUseDB()) {
					db.commit();
					db.close();
				}
			} catch (SQLException e1) {
				e1.printStackTrace();
			}

			// Treat an EarlyTerminationException differently
			if (e.getClass() == EarlyTerminationException.class) {
				ww.mainOutMessage(
						"Early termination by user request. Exiting with status 4."
								+ System.lineSeparator()
								+ "Logfile output: " + "error "
								+ format.format(new Date()), 2);
				System.exit(4);
			}
			ww.exceptionHandler(e, 3,
					"WR.collectData> Passing exit code 3, unknown exception");
		}
	}

	private HashMap<String, Integer> getZips() throws IOException,
			ClassNotFoundException, SQLException {
		ww.mainOutMessage("WR.getZips> Opening file...", 4);
		BufferedReader br = new BufferedReader(new FileReader(ZIPS_FILE));

		ww.mainOutMessage("WR.getZips> Initializing zips hashmap...", 4);
		HashMap<String, Integer> zipsMap = new HashMap<>();

		ww.mainOutMessage("WR.getZips> Reading file...", 4);
		String read = br.readLine();
		// Populate the zips hashmap
		while (read != null) {
			zipsMap.put(read, 0);
			ww.mainOutMessage("WR.getZips> File line:\"" + read + "\"", 5);
			read = br.readLine();
		}

		if (ww.isForceRun()) {
			ww.mainOutMessage(
					"WR.getZips> Returning successfully with all zips", 4);
			return zipsMap;
		}

		// Connect to database to determine which zip data has been successfully
		// collected today
		ww.mainOutMessage("WR.getZips> Connecting to database...", 4);
		String today = getYMDFormatter().format(
				(Calendar.getInstance()).getTimeInMillis());
		Connection con;
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		String connectionURL = "jdbc:sqlserver://MARCUSHANPC\\SQLEXPRESS;integratedSecurity=true;databaseName=weather;";
		con = DriverManager.getConnection(connectionURL);
		con.setAutoCommit(false);

		ww.mainOutMessage("WR.getZips> Executing query", 4);

		// Build and execute query
		String filler = "";
		if (ww.isSimRun())
			filler = "_alt";
		PreparedStatement ps = con
				.prepareStatement("SELECT * FROM weather.dbo.zips_collected"
						+ filler + " WHERE collected_date=?");
		ps.setString(1, today);
		ResultSet rs = ps.executeQuery();

		ww.mainOutMessage("WR.getZips> Populating hashmap", 4);
		while (rs.next()) {
			String zip = "";
			if (rs.getString(2).equals("80201")) {
				zip = "denver,co";
			} else {
				zip = rs.getString(2);
			}
			zipsMap.put(zip, rs.getInt(4));
		}

		ww.mainOutMessage(
				"WR.getZips> Returning successfully after parsing database", 4);
		return zipsMap;
	}

	private static SimpleDateFormat getYMDFormatter() {
		return new SimpleDateFormat("yyyy-MM-dd");
	}

	public synchronized void setFail() {
		anyFails = true;
	}

	/**
	 * @return the numThreads
	 */
	public synchronized int getNumThreads() {
		return ww.getNumThreads();
	}

	/**
	 * @return the ww
	 */
	public synchronized WeatherWrapper getWw() {
		return ww;
	}

	/**
	 * @return the startDate
	 */
	public synchronized Date getStartDate() {
		return ww.getStartDate();
	}

	/**
	 * @return the pastOnly
	 */
	public synchronized boolean isPastOnly() {
		return ww.isPastOnly();
	}

	/**
	 * @return the simRun
	 */
	public synchronized boolean isSimRun() {
		return ww.isSimRun();
	}

	/**
	 * @return the debug
	 */
	public synchronized boolean isDebug() {
		return ww.isDebug();
	}

	/**
	 * @return the useDB
	 */
	public synchronized boolean isUseDB() {
		return ww.isUseDB();
	}

	/**
	 * @return the ignoreLog
	 */
	public synchronized boolean isIgnoreLog() {
		return ww.isIgnoreLog();
	}

	public boolean isForceRun() {
		return ww.isForceRun();
	}

	/**
	 * @return the anyFails
	 */
	public synchronized boolean isAnyFails() {
		return anyFails;
	}

}
