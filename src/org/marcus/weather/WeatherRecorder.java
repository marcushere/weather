package org.marcus.weather;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
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

	private final WeatherWrapper ww;

	private final String LOG_NAME;
	private static final String ZIPS_FILE = "zips.txt";
	private final int numThreads;
	private final boolean pastOnly;
	private final boolean simRun;
	private final Date startDate;

	public final boolean debug;

	private final boolean useDB;
	private final boolean forceRun;
	private final boolean ignoreLog;

	private boolean anyFails = false;

	public WeatherRecorder(WeatherWrapper ww) {
		this.ww = ww;
		numThreads = ww.getNumThreads();
		pastOnly = ww.isPastOnly();
		if (pastOnly) {
			startDate = ww.getStartDate();
		} else {
			startDate = null;
		}
		simRun = ww.isSimRun();
		debug = ww.isDebug();
		useDB = ww.isUseDB();
		forceRun = ww.isForceRun();
		ignoreLog = ww.isIgnoreLog();
		LOG_NAME = ww.getLOG_NAME();
	}

	public void run() throws IOException {

		try {
			// create the zip map
			Map<String, Integer> zips = null;
			// populate the zip map
			try {
				if (pastOnly) {
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
			if (!zips.values().contains(0) && !forceRun) {
				ww.mainOutMessage("WR.run> All zips completed today", 4);
				ww.finish();
			}

			// Make the DBStore object
			DBStore db = null;
			if (useDB)
				db = new DBStore(forceRun, !simRun); // tell the database
														// object whether or not
														// to overwrite existing
														// rows
			// Make the CSVStore object
			CSVStore csv = null;
			if (!useDB)
				csv = new CSVStore();

			// Try to open the database connection twice before giving up
			for (int openDBint = 0; openDBint < 2; openDBint++) {
				ww.mainOutMessage("WR.run> Opening database connection...", 4);
				try {
					if (useDB)
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

			// Create threads container
			Thread[] zipHandlerThreads = new Thread[numThreads];
			try { // Create the DataFetcher object
				ww.mainOutMessage(
						"WR.run> Initializing zipHandlerThread objects...", 4);

				// Loop through the thread array
				for (int i = 0; i < zipHandlerThreads.length; i++) {
					ww.mainOutMessage("WR.run> Creating thread " + i + 1 + "/"
							+ zipHandlerThreads.length, 4);
					// Create the new threads
					if (useDB) {
						zipHandlerThreads[i] = new Thread(new ZipHandler(db,
								iter, i, ww));
					} else {
						zipHandlerThreads[i] = new Thread(new ZipHandler(csv,
								iter, i, ww));
					}
				}

				// If something went wrong with the thread creation
			} catch (Exception e) {
				FileWriter fileWriter;
				try {
					if (useDB) {
						db.commit();
						db.close();
					}
				} catch (IOException e1) {
					e1.printStackTrace();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}

				ww.mainOutMessage(
						"WR.run> Passing exit code 3, ZipHandler initialization failed",
						1);
			}

			// Start collecting datas
			try {
				for (int i = 0; i < zipHandlerThreads.length; i++) {
					// Start each of the threads
					ww.mainOutMessage("WR.run> Starting thread" + i + "/"
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
						if (useDB) {
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
							"WR.run> Data retrieval and storage parially successful",
							3);
				} else if (debug && !anyFails) {
					ww.mainOutMessage(
							"WR.run> Data retrieval and storage successful", 3);
				}

				if (useDB)
					ww.mainOutMessage("WR.run> Closing database connection", 4);
				if (useDB)
					db.close();

				ww.finish();

				// Catching exceptions thrown during data collection or storage
			} catch (Exception e) {
				FileWriter fileWriter;
				SimpleDateFormat format = getYMDFormatter();
				try {
					if (useDB) {
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
						"WR.run> Passing exit code 3, unknown exception");
			}

			// This could only be from an error message or the CSVStore object
			// initialization
		} catch (IOException e) {
			e.printStackTrace();
			ww.exceptionHandler(e, 4,
					"WR.main(unk) Passing exit code 4, IOException.");
		}

	}

	// /////////////////////////

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

		if (forceRun) {
			ww.mainOutMessage("WR.getZips> Returning successfully with all zips",4);
			return zipsMap;
		}
		
		//Connect to database to determine which zip data has been successfully collected today
		ww.mainOutMessage("WR.getZips> Connecting to database...",4);
		String today = getYMDFormatter().format(
				(Calendar.getInstance()).getTimeInMillis());
		Connection con;
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		String connectionURL = "jdbc:sqlserver://MARCUSHANPC\\SQLEXPRESS;integratedSecurity=true;databaseName=weather;";
		con = DriverManager.getConnection(connectionURL);
		con.setAutoCommit(false);

		ww.mainOutMessage("WR.getZips> Executing query",4);

		//Build and execute query
		String filler = "";
		if (simRun)
			filler = "_alt";
		PreparedStatement ps = con
				.prepareStatement("SELECT * FROM weather.dbo.zips_collected"
						+ filler + " WHERE collected_date=?");
		ps.setString(1, today);
		ResultSet rs = ps.executeQuery();

		ww.mainOutMessage("WR.getZips> Populating hashmap",4);
		while (rs.next()) {
			String zip = "";
			if (rs.getString(2).equals("80201")) {
				zip = "denver,co";
			} else {
				zip = rs.getString(2);
			}
			zipsMap.put(zip, rs.getInt(4));
		}

		ww.mainOutMessage("WR.getZips> Returning successfully after parsing database",4);
		return zipsMap;
	}

	private static SimpleDateFormat getYMDFormatter() {
		return new SimpleDateFormat("yyyy-MM-dd");
	}

	private static SimpleDateFormat getDateFormatter() {
		return new SimpleDateFormat("yyyy-MM-dd'T'kk:mm:ss.SSS");
	}

	private static void printError(Exception e, String zip) throws IOException {
		FileWriter fileWriter;
		PrintWriter out;
		fileWriter = new FileWriter(ERROR_NAME, true);
		out = new PrintWriter(fileWriter, true);

		out.print("error " + getDateFormatter().format(new Date()) + " " + zip);
		out.println();
		e.printStackTrace(out);
		out.print(e.getMessage());
		out.println();
		out.close();
		fileWriter.close();
	}

	public synchronized static void setFail() {
		anyFails = true;
	}

	/**
	 * @return the numThreads
	 */
	public static synchronized int getNumThreads() {
		return numThreads;
	}

	/**
	 * @return the pastOnly
	 */
	public static synchronized boolean isPastOnly() {
		return pastOnly;
	}

	/**
	 * @return the simRun
	 */
	public static synchronized boolean isSimRun() {
		return simRun;
	}

	/**
	 * @return the startDate
	 */
	public static synchronized Date getStartDate() {
		return startDate;
	}

	/**
	 * @return the debug
	 */
	public static synchronized boolean isDebug() {
		return debug;
	}

	/**
	 * @return the useDB
	 */
	public static synchronized boolean isUseDB() {
		return useDB;
	}

	/**
	 * @return the ignoreLog
	 */
	public static synchronized boolean isIgnoreLog() {
		return ignoreLog;
	}

	/**
	 * @return the anyFails
	 */
	public synchronized boolean isAnyFails() {
		return anyFails;
	}

	/**
	 * @param numThreads
	 *            the numThreads to set
	 */
	public static synchronized void setNumThreads(int numThreads) {
		WeatherRecorder.numThreads = numThreads;
	}

	/**
	 * @param pastOnly
	 *            the pastOnly to set
	 */
	public static synchronized void setPastOnly(boolean pastOnly) {
		WeatherRecorder.pastOnly = pastOnly;
	}

	/**
	 * @param simRun
	 *            the simRun to set
	 */
	public static synchronized void setSimRun(boolean simRun) {
		WeatherRecorder.simRun = simRun;
	}

	/**
	 * @param startDate
	 *            the startDate to set
	 */
	public static synchronized void setStartDate(Date startDate) {
		WeatherRecorder.startDate = startDate;
	}

	/**
	 * @param debug
	 *            the debug to set
	 */
	public static synchronized void setDebug(boolean debug) {
		WeatherRecorder.debug = debug;
	}

	/**
	 * @param useDB
	 *            the useDB to set
	 */
	public static synchronized void setUseDB(boolean useDB) {
		WeatherRecorder.useDB = useDB;
	}

	/**
	 * @param forceRun
	 *            the forceRun to set
	 */
	public static synchronized void setForceRun(boolean forceRun) {
		WeatherRecorder.forceRun = forceRun;
	}

	/**
	 * @param ignoreLog
	 *            the ignoreLog to set
	 */
	public static synchronized void setIgnoreLog(boolean ignoreLog) {
		WeatherRecorder.ignoreLog = ignoreLog;
	}

	/**
	 * @param anyFails
	 *            the anyFails to set
	 */
	public static synchronized void setAnyFails(boolean anyFails) {
		WeatherRecorder.anyFails = anyFails;
	}

	public static boolean isForceRun() {
		return forceRun;
	}

}
