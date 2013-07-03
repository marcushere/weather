package org.marcus.weather;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public abstract class WeatherWrapper {

	private int numThreads;
	private boolean pastOnly;
	private boolean simRun;
	private Date startDate;

	private int verbosity;
	private boolean debug;

	private boolean useDB;
	private boolean forceRun;
	private boolean ignoreLog;
	private String LOG_NAME;
	private static final String ERROR_NAME = "error.txt";
	private boolean stopProgram = false;

	private List<Integer> threads = null;

	private WeatherRecorder wr;

	public WeatherWrapper() {
		threads = new LinkedList<Integer>();
		wr = new WeatherRecorder(this);
	}

	public synchronized void finish() throws IOException {
		FileWriter fileWriter = new FileWriter(LOG_NAME, false);
		PrintWriter out = new PrintWriter(fileWriter, true);
		if (wr.isAnyFails()) {
			out.print("error " + getYMDFormatter().format(new Date()));
			mainOutMessage("WW.finish>" + System.lineSeparator() + "   error "
					+ getYMDFormatter().format(new Date()), 1);
		} else {
			out.print("ok " + getYMDFormatter().format(new Date()));
			mainOutMessage(
					"WW.finish> Logfile output:" + System.lineSeparator()
							+ "   ok " + getYMDFormatter().format(new Date()),
					3);
		}

		out.close();
		fileWriter.close();

		mainOutMessage("WW.finish> Passing exit code 0 (successful run)",3);
		System.exit(0);
	}

	public abstract void mainOutMessage(String str, int errorlevel);

	public abstract void threadOutMessage(String str, int threadID,
			int errorlevel);

	public synchronized void threadInit(int threadID) {
		threads.add(new Integer(threadID));
		threadOutMessage("Thread started", threadID, 1);
	}

	public synchronized void threadFinished(int threadID) {
		threadOutMessage("Thread finished", threadID, 1);
	}

	public synchronized void exceptionHandler(Exception e, int exitCode,
			String outputMessage) throws IOException {
		if (exitCode > -1) {
			FileWriter fileWriter;
			fileWriter = new FileWriter(LOG_NAME, false);
			PrintWriter out = new PrintWriter(fileWriter, true);
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			out.print("error " + format.format(new Date()));

			out.close();
			printError(e, "Output message: " + outputMessage);
			mainOutMessage(outputMessage, 1);
			System.exit(exitCode);
		} else {
			printError(e, "Output message: " + outputMessage);
			mainOutMessage(outputMessage, -exitCode);
		}
	}

	@SuppressWarnings("unused")
	private void writeLogfile(String log) {

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

	private static void printError(Exception e, String zip) throws IOException {
		FileWriter fileWriter;
		PrintWriter out;
		fileWriter = new FileWriter(ERROR_NAME, true);
		out = new PrintWriter(fileWriter, true);

		out.print("error "
				+ (new SimpleDateFormat("yyyy-MM-dd'T'kk:mm:ss.SSS"))
						.format(new Date()) + " " + zip);
		out.println();
		e.printStackTrace(out);
		out.print(e.getMessage());
		out.println();
		out.close();
		fileWriter.close();
	}

	private static SimpleDateFormat getYMDFormatter() {
		return new SimpleDateFormat("yyyy-MM-dd");
	}

	/**
	 * @return the stopProgram
	 */
	public boolean isStopProgram() {
		return stopProgram;
	}

}
