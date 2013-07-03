package org.marcus.weather;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class WeatherTerm extends WeatherWrapper {

	private int numThreads;
	private boolean pastOnly;
	private boolean simRun;
	private Date startDate;

	private int verbosity;
	private boolean debug;

	private boolean useDB;
	private boolean forceRun;
	private boolean ignoreLog;
	private String LOG_NAME = "log.txt";

	/**
	 * @param args
	 */
	public WeatherTerm(String[] args) {

		// parse the arguments
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
						printHelpMessage();
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

		// give messages saying run conditions
		if (forceRun) {
			mainOutMessage("WT> Forced run", 1);
		} else {
			mainOutMessage("WT> Normal run", 1);
		}
		if (useDB) {
			mainOutMessage("WT> Using database", 1);
			if (!simRun) {
				mainOutMessage("WT> Writing to standard table in database", 1);
			} else {
				mainOutMessage("WT> Writing to alternate table in database", 1);
				LOG_NAME = LOG_NAME + ".sim";
			}
		} else {
			mainOutMessage("WT> Writing to CSV", 1);
		}
		if (pastOnly) {
			mainOutMessage("WT> Collecting past data starting at "
					+ getYMDFormatter().format(startDate), 1);
		} else {
			mainOutMessage("WT> Collecting today's data only", 1);
		}
		if (!ignoreLog) {
			mainOutMessage("WT> Obeying run restrictions", 1);
		} else {
			mainOutMessage("WT> Ignoring run restrictions", 1);
		}
		if (numThreads == 1) {
			mainOutMessage("WT> Running with only one thread", 1);
		} else {
			mainOutMessage("WT> Multithreaded with numThreads = " + numThreads,
					1);
		}
	}

	/**
	 * 
	 */
	public void run() {
		try {
			if(!ignoreLog&&!finishedToday()){
				
			}else{
				mainOutMessage("Already finished today", 1);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		CheckKeyboard ck = new CheckKeyboard();
		Thread thread = new Thread(ck);
		thread.start();
	}
	
	/**
	 * 
	 */
	private boolean finishedToday() throws IOException {
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

	/**
	 * @see org.marcus.weather.WeatherWrapper#mainOutMessage(java.lang.String,
	 *      int)
	 */
	public synchronized void mainOutMessage(String str, int errorlevel) {
		if(errorlevel<=verbosity) System.out.println(str);
	}

	/**
	 * @see org.marcus.weather.WeatherWrapper#threadOutMessage(java.lang.String,
	 *      int, int)
	 */
	public synchronized void threadOutMessage(String str, int threadID, int errorlevel) {
		if(errorlevel<=verbosity)System.out.println("Thread "+threadID+"> " +str);
	}
	
	/**
	 * @return the numThreads
	 */
	public synchronized int getNumThreads() {
		return numThreads;
	}

	/**
	 * @return the pastOnly
	 */
	public synchronized boolean isPastOnly() {
		return pastOnly;
	}

	/**
	 * @return the simRun
	 */
	public synchronized boolean isSimRun() {
		return simRun;
	}

	/**
	 * @return the startDate
	 */
	public synchronized Date getStartDate() {
		return startDate;
	}

	/**
	 * @return the verbosity
	 */
	public synchronized int getVerbosity() {
		return verbosity;
	}

	/**
	 * @return the debug
	 */
	public synchronized boolean isDebug() {
		return debug;
	}

	/**
	 * @return the useDB
	 */
	public synchronized boolean isUseDB() {
		return useDB;
	}

	/**
	 * @return the forceRun
	 */
	public synchronized boolean isForceRun() {
		return forceRun;
	}

	/**
	 * @return the ignoreLog
	 */
	public synchronized boolean isIgnoreLog() {
		return ignoreLog;
	}

	/**
	 * @return the lOG_NAME
	 */
	public synchronized String getLOG_NAME() {
		return LOG_NAME;
	}

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

}
