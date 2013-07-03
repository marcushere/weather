package org.marcus.weather;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;

public class WeatherTerm extends WeatherWrapper {

	/**
	 * @param args
	 */
	public WeatherTerm(String[] args) {

		LOG_NAME = "log.txt";
		
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
					SimpleDateFormat format = super.getYMDFormatter();
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
					LOG_NAME = "log_alt.txt";
				} else if (args[i].equals("-i")) {
					ignoreLog = true;
				} else if (args[i].startsWith("-t")) {
					if (args[i].length() > 2) {
						numThreads = Integer.parseInt(args[i].substring(2));
					} else {
						numThreads = 4;
					}
				} else if (args[i].equals("--term")) {

				} else if (args[i].isEmpty()) {

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
					+ super.getYMDFormatter().format(startDate), 1);
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

		threads = new LinkedList<Integer>();
		wr = new WeatherRecorder(this);
	}

	/**
	 * @throws IOException
	 * 
	 */
	public void run() throws IOException {
		try {
			if (ignoreLog) {

			} else if (finishedToday()){
				mainOutMessage("Already finished today", 1);
			}
		} catch (IOException e) {
			exceptionHandler(e, 1,
					"WT.run> Unknown error, probably a problem reading logfile");
		}

		CheckKeyboard ck = new CheckKeyboard(this);
		Thread thread = new Thread(ck);
		thread.start();

		wr.run();
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
			String now = super.getYMDFormatter().format(new Date());
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
		if (errorlevel <= verbosity)
			System.out.println(str);
	}

	/**
	 * @see org.marcus.weather.WeatherWrapper#threadOutMessage(java.lang.String,
	 *      int, int)
	 */
	public synchronized void threadOutMessage(String str, int threadID,
			int errorlevel) {
		if (errorlevel <= verbosity)
			System.out.println("Thread " + threadID + "> " + str);
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
