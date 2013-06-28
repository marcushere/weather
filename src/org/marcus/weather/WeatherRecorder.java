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

public class WeatherRecorder {

	private static class EarlyTerminationException extends Exception {
		private static final long serialVersionUID = 7107342102877398736L;
	}

	private static final String LOG_NAME = "log.txt";
	private static final String ZIPS_FILE = "zips.txt";
	private static final String ERROR_NAME = "error.txt";
	private static int verbosity = 1;
	private static boolean debug = false;
	private static boolean pastOnly = false;
	private static boolean writeToDB = true;
	private static Date startDate = null;

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
				} else if (args[i].equals("help")) {
					System.out
							.println("Arguments are:"
									+ System.lineSeparator()
									+ "-csv to write to csv files"
									+ System.lineSeparator()
									+ "-d[#] to debug with verbosity level #"
									+ System.lineSeparator()
									+ "-pastYYYY-MM-DD"
									+ System.lineSeparator()
									+ "-f to force run"
									+ System.lineSeparator()
									+ "-i to ignore run restrictions"
									+ System.lineSeparator()
									+ "-s to simulate run (writing to alternate tables)");
					System.exit(0);
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
				} else if (args[i].equals("-s")) {
					writeToDB = false;
				} else if (args[i].equals("-i")) {
					ignoreLog = true;
				} else {
					System.out
							.println("Arguments are:"
									+ System.lineSeparator()
									+ " -csv to write to csv files"
									+ System.lineSeparator()
									+ "-d[#] to debug with verbosity level #"
									+ System.lineSeparator()
									+ "-pastYYYY-MM-DD"
									+ System.lineSeparator()
									+ "-f to force run"
									+ System.lineSeparator()
									+ "-i to ignore run restrictions"
									+ System.lineSeparator()
									+ "-s to simulate run (writing to alternate tables)");
					System.exit(0);
				}
			}
		}

		if (verbosity > 0) {
			if (forceRun) {
				System.out.println("WR.main(1/8.1/4) Forced run");
			} else {
				System.out.println("WR.main(1/8.1/4) Normal run");
			}
			if (useDB) {
				System.out.println("WR.main(1/8.2/4) Using database");
				if (writeToDB) {
					System.out
							.println("WR.main(1/8.2/4.1/1) Writing to standard table in database");
				} else {
					System.out
							.println("WR.mail(1/8.2/4.1/1) Writing to alternate table in database");
				}
			} else {
				System.out.println("WR.main(1/8.2/4) Writing to CSV");
			}
			if (pastOnly) {
				System.out
						.println("WR.main(1/8.3/4) Collecting past data starting at "
								+ getYMDFormatter().format(startDate));
			} else {
				System.out
						.println("WR.main(1/8.3/4) Collecting today's data only");
			}
			if (!ignoreLog) {
				System.out.println("WR.main(1/8.4/4) Obeying run restrictions");
			} else {
				System.out
						.println("WR.main(1/8.4/4) Ignoring run restrictions");
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

			DBStore db = null;
			if (useDB)
				db = new DBStore(forceRun, !writeToDB); // tell the database
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

			String currentZip = "";

			DataFetcher df = null;
			try { // create the DataFetcher object
				if (debug)
					System.out
							.println("WR.main(4/8.2/2 Creating DataFetcher object...");
				df = new DataFetcher(false);
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
				Iterator<String> iter = zips.keySet().iterator();
				int zipNumber = 1;
				int totalZips = zips.size();
				while (iter.hasNext()) {
					currentZip = iter.next();
					if (!currentZip.equals(Integer.parseInt("1"))) {
						if (!pastOnly) { // do this if doing a normal daily data
											// collection
							if (verbosity < 2) {
								System.out.println(currentZip + " ("
										+ zipNumber + "/" + totalZips + ")");
							} else { // level 3 message
								System.out
										.println("WR.main(5/8) Collecting data for "
												+ currentZip
												+ " ("
												+ zipNumber
												+ "/" + totalZips + ")");
							}
							df.load(currentZip); // get the data
							if (useDB) { // for if the database is being used
								if (df.valid) { // for when the collection was
												// successful
									if (verbosity > 2)
										System.out
												.println("WR.main(5/8.1/5) Storing data for "
														+ currentZip
														+ " ("
														+ zipNumber
														+ "/"
														+ totalZips + ")");
									if (verbosity > 3)
										System.out
												.println("WR.main(5/8.2/5) Storing forecast1 for "
														+ currentZip
														+ " ("
														+ zipNumber
														+ "/"
														+ totalZips + ")");
									db.storeForecast(df.forecast1);
									if (verbosity > 3)
										System.out
												.println("WR.main(5/8.3/5) Storing forecast2 for "
														+ currentZip
														+ " ("
														+ zipNumber
														+ "/"
														+ totalZips + ")");
									db.storeForecast(df.forecast3);
									if (verbosity > 3)
										System.out
												.println("WR.main(5/8.4/5) Storing history data for "
														+ currentZip
														+ " ("
														+ zipNumber
														+ "/"
														+ totalZips + ")");
									db.storePast(df.past);
									if (verbosity > 3)
										System.out
												.println("WR.main(5/8.5/5) Updating success for "
														+ currentZip
														+ " ("
														+ zipNumber
														+ "/"
														+ totalZips + ")");
									db.updateZipSuccess(currentZip);
								} else if (!df.valid) { // for when the
														// collection
														// was unsuccessful
									if (debug)
										System.out
												.println("WR.main(5/8.1/1) Fail on "
														+ currentZip
														+ " ("
														+ zipNumber
														+ "/"
														+ totalZips + ")");
									db.updateZipFail(currentZip);
									anyFails = true;
								}
								if (verbosity > 3)
									System.out
											.println("WR.main(5/8.6/6) Attempting commit");
								db.commit();
							} else if (df.valid && !useDB) { // if using csv
								if (verbosity > 2)
									System.out
											.println("WR.main(5/8.1/4) Storing data for "
													+ currentZip
													+ " ("
													+ zipNumber
													+ "/"
													+ totalZips + ")");
								if (verbosity > 3)
									System.out
											.println("WR.main(5/8.2/4) Storing forecast1 for "
													+ currentZip
													+ " ("
													+ zipNumber
													+ "/"
													+ totalZips + ")");
								csv.storeForecast(df.forecast1);
								if (verbosity > 3)
									System.out
											.println("WR.main(5/8.3/4) Storing forecast2 for "
													+ currentZip
													+ " ("
													+ zipNumber
													+ "/"
													+ totalZips + ")");
								csv.storeForecast(df.forecast3);
								if (verbosity > 3)
									System.out
											.println("WR.main(5/8.4/4) Storing history data for "
													+ currentZip
													+ " ("
													+ zipNumber
													+ "/"
													+ totalZips + ")");
								csv.storePast(df.past);
							}
							if (ck.isStopProgram()) { // if user requested early
														// termination
								while (iter.hasNext()) {
									currentZip = (String) iter.next();
									db.updateZipFail(currentZip + " ("
											+ zipNumber + "/" + totalZips + ")");
									anyFails = true;
								}
								throw new EarlyTerminationException();
							}
						} else {
							// if pastOnly cycle through the dates up till
							// yesterday
							// (not working)
							System.out.println(currentZip);
							Calendar cal = Calendar.getInstance();
							Date today = cal.getTime();
							cal.setTime(startDate);
							while (cal.getTime().before(today)) {
								Date currentDate = cal.getTime();
								if (debug)
									System.out.println(getYMDFormatter()
											.format(currentDate)
											+ " "
											+ getYMDFormatter().format(today));
								df.loadPast(currentZip, currentDate);
								db.storePast(df.past);
								cal.setTimeInMillis(cal.getTimeInMillis() + 3600 * 24 * 1000);
							}
						}
					} else {
						if (verbosity > 3)
							System.out.println("WR.main(5/8) skipping "
									+ currentZip + " (" + zipNumber + "/"
									+ totalZips + ")");
					}
					zipNumber++;
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
							.println("WR.main(unk) Passing exit code 3, unknown exception on zip "
									+ currentZip);
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
			if (!writeToDB)
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

	public static void setFail(){
		anyFails = true;
	}
	
}
