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

	private static String LOG_NAME = "log";
	private static final String ZIPS_FILE = "zips.txt";
	private static final String ERROR_NAME = "error.txt";
	private static int numThreads = 1;
	private static boolean pastOnly = false;
	private static boolean simRun = false;
	private static Date startDate = null;

	public static int verbosity = 1;
	public static boolean debug = false;

	private static boolean useDB = true;
	private static boolean forceRun = false;
	private static boolean ignoreLog = false;

	private static boolean anyFails = false;

	public static void main(String[] args) {
		CheckKeyboard ck = new CheckKeyboard();
		Thread thread = new Thread(ck);
		thread.start();
		if (args.length > 0) {
			for (int i = 0; i < args.length; i++) {
				if (args[i].equals("-csv")) {
					useDB = false;
				} else if (args[i].equals("-help")) {
					printHelpMessage();
				} else if (args[i].startsWith("-d")) {
					debug = true;
					if (args[i].length() > 2) {
						verbosity = Integer.parseInt(args[i].substring(2));
					} else {
						verbosity = 1;
					}
					System.out
							.println("WR.main(1/8) Debug on with verbosity of "
									+ verbosity);
				} else if (args[i].startsWith("-past")) {
					pastOnly = true;
					SimpleDateFormat format = getYMDFormatter();
					try {
						startDate = format.parse(args[i].substring(5));
					} catch (ParseException e) {
						System.out
								.println("Date must be in format 'yyyy-mm-dd'");
						System.exit(0);
					}
				} else if (args[i].equals("-f")) {
					forceRun = true;
					ignoreLog = true;
				} else if (args[i].equals("-s")) {
					simRun = true;
				} else if (args[i].equals("-i")) {
					ignoreLog = true;
				} else if (args[i].startsWith("-t")) {
					if (args[i].length() > 2) {
						numThreads = Integer.parseInt(args[i].substring(2));
					} else {
						numThreads = 4;
					}
				} else {
					printHelpMessage();
				}
			}
		}

		if (simRun) {
			LOG_NAME = LOG_NAME + "_alt.txt";
		} else {
			LOG_NAME = LOG_NAME + ".txt";
		}

		if (verbosity > 0) {
			if (forceRun) {
				System.out.println("WR.main(1/8.1/5) Forced run");
			} else {
				System.out.println("WR.main(1/8.1/5) Normal run");
			}
			if (useDB) {
				System.out.println("WR.main(1/8.2/5.1/2) Using database");
				if (!simRun) {
					System.out
							.println("WR.main(1/8.2/5.2/2) Writing to standard table in database");
				} else {
					System.out
							.println("WR.mail(1/8.2/5.2/2) Writing to alternate table in database");
				}
			} else {
				System.out.println("WR.main(1/8.2/5) Writing to CSV");
			}
			if (pastOnly) {
				System.out
						.println("WR.main(1/8.3/5) Collecting past data starting at "
								+ getYMDFormatter().format(startDate));
			} else {
				System.out
						.println("WR.main(1/8.3/5) Collecting today's data only");
			}
			if (!ignoreLog) {
				System.out.println("WR.main(1/8.4/5) Obeying run restrictions");
			} else {
				System.out
						.println("WR.main(1/8.4/5) Ignoring run restrictions");
			}
			if (numThreads==1){
				System.out
				.println("WR.main(1/8.5/5) Running with only one thread");
			} else {
				System.out
				.println("WR.main(1/8.5/5) Multithreaded with numThreads = "
						+ numThreads);
			}
		}

		try {
			Map<String, Integer> zips = null;
			try {
				if (pastOnly) {
					if (debug)
						System.out
								.println("WR.main(2/8) Retrieving all zip codes for past data aquisition...");
					zips = getZips();
				} else {
					if (debug)
						System.out
								.println("WR.main(2/8) Checking for previous runs today...");
					if (finishedToday() && forceRun == false) {
						System.out.println("Already run today");
						System.exit(0);
					}
					if (debug)
						System.out
								.println("WR.main(2/8) Retrieving zip codes...");
					zips = getZips();
				}
			} catch (SQLException e) {
				FileWriter fileWriter;
				fileWriter = new FileWriter(LOG_NAME, false);
				PrintWriter out = new PrintWriter(fileWriter, true);
				SimpleDateFormat format = getYMDFormatter();
				out.print("error " + format.format(new Date()));

				out.close();
				printError(e, "");
				System.out.println("Error 2: no db connection");
				System.exit(2);
			} catch (Exception e) {
				printError(e, "");
				System.out.println("Error 1: unknown error");
				System.exit(1);
			}

			if (debug)
				System.out
						.println("WR.main(3/8) Zip code retrieval finished, creating storage object");

			if (!zips.values().contains(0) && !forceRun) {
				System.out.println("WR.main(check/8)All zips completed today");
				finish();
			}

			DBStore db = null;
			if (useDB)
				db = new DBStore(forceRun, !simRun); // tell the database
														// object whether or not
														// to overwrite existing
														// rows
			CSVStore csv = null;
			if (!useDB)
				csv = new CSVStore();

			for (int openDBint = 0; openDBint < 2; openDBint++) { // try to open
																	// the
																	// database
				// connection up to two times
				// before giving up
				if (debug)
					System.out
							.println("WR.main(3/8.1/1) Opening database connection...");
				try {
					if (useDB)
						db.open();
					break; // break out of the loop if the database connection
							// opens successfully
				} catch (Exception e) {
					FileWriter fileWriter;
					fileWriter = new FileWriter(LOG_NAME, false);
					PrintWriter out = new PrintWriter(fileWriter, true);
					SimpleDateFormat format = getYMDFormatter();
					out.print("error " + format.format(new Date()));

					out.close();
					printError(e, "");
					System.out.println("Error 2: no db connection");
					System.exit(2);
				}
			}

			if (debug)
				System.out
						.println("WR.main(4/8.1/2) Storage object ready, beginning to collect and store data...");

			Iterator<Entry<String, Integer>> iter = zips.entrySet().iterator();

			Thread[] zipHandlerThreads = new Thread[numThreads];
			try { // create the DataFetcher object
				if (debug)
					System.out
							.println("WR.main(4/8.2/20 Initializing zipHandlerThread objects...");
				for (int i = 0; i < zipHandlerThreads.length; i++) {
					if (debug)
						System.out.println("WR.main(5/8) Creating thread " + i
								+ 1 + "/" + zipHandlerThreads.length);
					if (useDB) {
						zipHandlerThreads[i] = new Thread(new ZipHandler(db,
								iter, i));
					} else {
						zipHandlerThreads[i] = new Thread(new ZipHandler(csv,
								iter, i));
					}
				}
			} catch (Exception e) {
				FileWriter fileWriter;
				try {
					if (useDB) {
						db.commit();
						db.close();
					}
					fileWriter = new FileWriter(LOG_NAME, false);
					PrintWriter out = new PrintWriter(fileWriter, true);

					SimpleDateFormat format = getYMDFormatter();

					out.print("error " + format.format(new Date()));

					out.close();
					fileWriter.close();
					printError(e, "");
				} catch (IOException e1) {
					e1.printStackTrace();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}

				if (debug)
					System.out
							.println("WR.main(unk) Passing exit code 3, df initialization failed");
				System.exit(3);
			}
			try {
				// start collecting datas
				for (int i = 0; i < zipHandlerThreads.length; i++) {
					if (debug)
						System.out.println("WR.main(5/8) Starting thread" + i
								+ "/" + zipHandlerThreads.length);
					zipHandlerThreads[i].start();
				}

				boolean stillRunning = true;

				while (stillRunning) {
					stillRunning = false;
					for (int i = 0; i < zipHandlerThreads.length; i++) {
						if (zipHandlerThreads[i].isAlive()) {
							stillRunning = true;
							zipHandlerThreads[i]
									.join(4000 / zipHandlerThreads.length);
						}
					}

					if (ck.isStopProgram()) { // if user requested early
												// termination
						synchronized (iter) {
							while (iter.hasNext()) {
								String currentZip = iter.next().getKey();
								db.updateZipFail(currentZip);
								anyFails = true;
							}
						}
						throw new EarlyTerminationException();
					}
					Thread.sleep(1000);
				}

				if (debug && anyFails) {
					System.out
							.println("WR.main(6/8) Data retrieval and storage parially successful");
				} else if (debug && !anyFails) {
					System.out
							.println("WR.main(6/8) Data retrieval and storage successful");
				}

				if (debug && useDB)
					System.out
							.println("WR.main(6/8.1/1) Closing database connection");
				if (useDB)
					db.close();

				finish();
			} catch (Exception e) {
				FileWriter fileWriter;
				SimpleDateFormat format = getYMDFormatter();
				try {
					if (useDB) {
						db.commit();
						db.close();
					}
					fileWriter = new FileWriter(LOG_NAME, false);
					PrintWriter out = new PrintWriter(fileWriter, true);

					out.print("error " + format.format(new Date()));

					out.close();
					fileWriter.close();
					printError(e, "");
				} catch (IOException e1) {
					e1.printStackTrace();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}

				if (e.getClass() == EarlyTerminationException.class) {
					System.out
							.println("Early termination by user request. Exiting with status 4."
									+ System.lineSeparator()
									+ "Logfile output: "
									+ "error "
									+ format.format(new Date()));
					System.exit(4);
				}
				if (debug)
					System.out
							.println("WR.main(unk) Passing exit code 3, unknown exception");
				System.exit(3);
			}
		} catch (IOException e) {
			e.printStackTrace();
			if (debug)
				System.out
						.println("WR.main(unk) Passing exit code 4, IOException."
								+ System.lineSeparator()
								+ "Logfile probably unreadable");
			System.exit(4);
		}

	}

	/**
	 * @throws IOException
	 *             Prints the log file and a debug message if appropriate
	 * 
	 */
	private static void finish() throws IOException {
		FileWriter fileWriter = new FileWriter(LOG_NAME, false);
		PrintWriter out = new PrintWriter(fileWriter, true);
		if (anyFails) {
			out.print("error " + getYMDFormatter().format(new Date()));
			if (debug)
				System.out.println("WR.main(7/8) Logfile output:"
						+ System.lineSeparator() + "   error "
						+ getYMDFormatter().format(new Date()));
		} else {
			out.print("ok " + getYMDFormatter().format(new Date()));
			if (debug)
				System.out.println("WR.main(7/8) Logfile output:"
						+ System.lineSeparator() + "   ok "
						+ getYMDFormatter().format(new Date()));
		}

		out.close();
		fileWriter.close();

		if (debug)
			System.out
					.println("WR.main(8/8) Passing exit code 0 (successful run)");
		System.exit(0);
	}

	/**
	 * 
	 */
	private static void printHelpMessage() {
		System.out
				.println("Arguments are:"
						+ System.lineSeparator()
						+ "-csv to write to csv files"
						+ System.lineSeparator()
						+ "-d[#] to debug with verbosity level #"
						// + System.lineSeparator() + "-pastYYYY-MM-DD"
						+ System.lineSeparator()
						+ "-f to force run with all zips, forces -i"
						+ System.lineSeparator()
						+ "-i to ignore run restrictions"
						+ System.lineSeparator()
						+ "-s to simulate run (writing to alternate tables)"
						+ System.lineSeparator()
						+ "-t[#] to multithread the data aquaisition (default 4 threads)"
						+ System.lineSeparator()
						+ "-help to display this help message");
		System.exit(0);
	}

	private static boolean finishedToday() throws IOException {
		// check for ignoreLog
		if (ignoreLog) {
			if (verbosity > 1)
				System.out.println(">WR.finishedToday(1/1) Ignoring log file");
			return false;
		}
		// don't want it to run before 4am
		int hour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
		if (hour < 4)
			return true;
		// otherwise, look at the log file
		BufferedReader br = new BufferedReader(new FileReader(LOG_NAME));
		String line = br.readLine();
		if (line == null) {
			return false;
		}

		String[] pieces = line.split(" ");
		if (pieces.length == 2 && pieces[0].equals("ok")) {
			String now = getYMDFormatter().format(new Date());
			if (pieces[1].equals(now)) {
				return true;
			}
		} else if (pieces[0].equals("stop")) {
			return true;
		}
		return false;
	}

	private static HashMap<String, Integer> getZips() throws IOException,
			ClassNotFoundException, SQLException {
		if (debug)
			System.out.println(">WR.getZips(1/3) Opening file...");
		BufferedReader br = new BufferedReader(new FileReader(ZIPS_FILE));
		if (verbosity > 3)
			System.out
					.println(">WR.getZips(1/3.1/2) Initializing zips hashmap...");
		HashMap<String, Integer> zipsHash = new HashMap<>();

		if (verbosity > 3)
			System.out.println(">WR.getZips(1/3.2/2) Reading file...");
		String read = br.readLine();
		while (read != null) {
			zipsHash.put(read, 0);
			if (verbosity > 4)
				System.out.println(read);
			read = br.readLine();
		}

		if (debug)
			System.out
					.println(">WR.getZips(2/3) Checking for previous runs today...");

		if (!finishedToday()) {
			if (debug)
				System.out
						.println(">WR.getZips(2/3.1/2) Connecting to database...");
			String today = getYMDFormatter().format(
					(Calendar.getInstance()).getTimeInMillis());
			Connection con;
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			String connectionURL = "jdbc:sqlserver://MARCUSHANPC\\SQLEXPRESS;integratedSecurity=true;databaseName=weather;";
			con = DriverManager.getConnection(connectionURL);
			con.setAutoCommit(false);

			if (debug)
				System.out.println(">WR.getZips(2/3.2/2) Executing query");

			String filler = "";
			if (simRun)
				filler = "_alt";
			PreparedStatement ps = con
					.prepareStatement("SELECT * FROM weather.dbo.zips_collected"
							+ filler + " WHERE collected_date=?");
			ps.setString(1, today);
			ResultSet rs = ps.executeQuery();

			if (debug)
				System.out.println(">WR.getZips(2/3.3/3) Populating hashmap");
			while (rs.next()) {
				String zip = "";
				if (rs.getString(2).equals("80201")) {
					zip = "denver,co";
				} else {
					zip = rs.getString(2);
				}
				zipsHash.put(zip, rs.getInt(4));
			}

			if (debug)
				System.out
						.println(">WR.getZips(3/3) Returning successfully after parsing database");
			return zipsHash;
		} else if (forceRun) {
			if (debug)
				System.out
						.println(">WR.getZips(3/3) Returning successfully with all zips");
			return zipsHash;
		} else {
			if (debug)
				System.out
						.println(">WR.getZips(3/3) Returning successfully with no zips");
			return new HashMap<String, Integer>();
		}

		// String lastZip = "";
		// BufferedReader br = new BufferedReader(new FileReader(LOG_NAME));
		// String line = br.readLine();
		// if (line == null) {
		// lastZip = "";
		// } else {
		// String[] splitted = line.split(" ");
		//
		// if (splitted.length > 2) {
		// lastZip = splitted[2];
		// } else if (splitted[1].equals(getYMDFormatter().format(new Date())))
		// {
		// lastZip = "";
		// }
		// }
		//
		// boolean pastLastZip = false;
		// br = new BufferedReader(new FileReader(ZIPS_FILE));
		// line = "";
		// String read = br.readLine();
		// while (read != null) {
		// if (lastZip.isEmpty()) {
		// if (line.isEmpty()) {
		// line = read;
		// } else {
		// line = line + " " + read;
		// }
		// } else if (!lastZip.isEmpty() && pastLastZip) {
		// if (line.isEmpty()) {
		// line = read;
		// } else {
		// line = line + " " + read;
		// }
		// } else if (!lastZip.isEmpty() && !pastLastZip) {
		// if (read.equals(lastZip)) {
		// pastLastZip = true;
		// line = read;
		// }
		// }
		// read = br.readLine();
		// }
		// return line.split(" ");
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

	public static boolean isForceRun() {
		return forceRun;
	}

}
