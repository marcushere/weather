package org.marcus.weather;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public abstract class WeatherWrapper {

	protected int numThreads = 4;
	protected boolean pastOnly = false;
	protected boolean simRun = false;
	protected Date startDate;

	protected int verbosity = 1;
	protected boolean debug = false;

	protected boolean useDB = true;
	protected boolean forceRun = false;
	protected boolean ignoreLog = false;
	protected String LOG_NAME;
	protected static final String ERROR_NAME = "error.txt";
	protected boolean stopProgram = false;

	protected List<Integer> threads = null;

	protected WeatherRecorder wr;
	
	public abstract void run() throws IOException;

	protected synchronized void finish() throws IOException {
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

	protected abstract void mainOutMessage(String str, int errorlevel);

	protected abstract void threadOutMessage(String str, int threadID,
			int errorlevel);

	protected synchronized void threadInit(int threadID) {
		threads.add(new Integer(threadID));
		threadOutMessage("Thread started", threadID, 1);
	}

	protected synchronized void threadFinished(int threadID) {
		threadOutMessage("Thread finished", threadID, 1);
	}

	protected synchronized void exceptionHandler(Exception e, int exitCode,
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

	protected static SimpleDateFormat getYMDFormatter() {
		return new SimpleDateFormat("yyyy-MM-dd");
	}

	@SuppressWarnings("unused")
	private void writeLogfile(String log) {

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

	/**
	 */
	public synchronized void stopProgram() {
		this.stopProgram = true;
	}

	/**
	 * @return the stopProgram
	 */
	public boolean isStopProgram() {
		return stopProgram;
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

}
