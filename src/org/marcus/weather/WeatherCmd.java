package org.marcus.weather;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class WeatherCMD {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		Config config = parseArgs(args);

		try {
			if (config.isRunTerminal()) {
				new WeatherTerm(config).run();
			} else {
				// ww = new WeatherGUI(args);
			}
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}

	}

	private static Config parseArgs(String[] args) {
		Config config = new Config();
		if (args.length > 0) {
			for (int i = 0; i < args.length; i++) {
				if (args[i].equals("-h")) {
					printHelpMessage();
					System.exit(0);
				} else if (args[i].startsWith("-d")) {
					config.setDebug(true);
					if (args[i].length() > 2) {
						config.setVerbosity(Integer.parseInt(args[i]
								.substring(2)));
					} else {
						config.setVerbosity(1);
					}
				} else if (args[i].equals("--term")) {
					config.setRunTerminal(true);
				} else if (args[i].startsWith("-past")) {
					config.setPastOnly(true);
					SimpleDateFormat format = config.getYMDFormatter();
					try {
						config.setStartDate(format.parse(args[i].substring(5)));
					} catch (ParseException e) {
						printHelpMessage();
					}
				} else if (args[i].equals("-f")) {
					config.setForceRun(true);
					config.setIgnoreLog(true);
				} else if (args[i].equals("-s")) {
					config.setSimRun(true);
					config.setLOG_NAME("log_alt.txt");
				} else if (args[i].equals("-i")) {
					config.setIgnoreLog(true);
				} else if (args[i].startsWith("-t")) {
					if (args[i].length() > 2) {
						config.setNumThreads(Integer.parseInt(args[i]
								.substring(2)));
					} else {
						config.setNumThreads(4);
					}
				} else if (args[i].equals("--term")) {

				}else if(args[i].equals("--update")){
					config.setForceRun(true);
					config.setIgnoreLog(true);
					config.setUpdate(true);
				} else if (args[i].isEmpty()) {

				} else {
					printHelpMessage();
					System.exit(0);
				}
				if(config.isUpdate()&&(config.isSimRun()||config.isPastOnly())){
					printHelpMessage();
					System.exit(0);
				}
			}
		}
		return config;
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
