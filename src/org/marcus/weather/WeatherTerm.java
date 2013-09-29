package org.marcus.weather;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;

import org.marcus.weather.process.UpdateDB;

public class WeatherTerm implements WeatherUI {

	private WeatherRecorder wr;
	private final Config config;
	private UpdateDB updateDB;

	public WeatherTerm(Config config) {
		this.config = config;
		if (config.isUpdate()) {
			this.updateDB = new UpdateDB(this);
		} else {
			this.wr = new WeatherRecorder(this, config);
		}
	}

	public void run() throws IOException {
		config.outputConfig(this);
		try {
			if (config.isIgnoreLog()) {

			} else if (config.finishedToday()) {
				mainOutMessage("Already finished today", 1);
			}
		} catch (IOException e) {
			Util.exceptionHandler(
					e,
					1,
					"WT.run> Unknown error, probably a problem reading logfile",
					config, this);
		}

		CheckKeyboard ck = new CheckKeyboard(this);
		Thread ckThread = new Thread(ck);
		ckThread.start();

		if (config.isUpdate()) {
			updateDB.run();
		} else {
			wr.run();
		}
	}

	public synchronized void mainOutMessage(String str, int errorlevel) {
		if (errorlevel <= config.getVerbosity())
			System.out.println(str);
	}

	/**
	 * @see org.marcus.weather.WeatherUI#threadOutMessage(java.lang.String, int,
	 *      int)
	 */
	public synchronized void threadOutMessage(String str, int threadID,
			int errorlevel) {
		if (errorlevel <= config.getVerbosity())
			System.out.println("Thread " + threadID + "> " + str);
	}

	public synchronized void finish() throws IOException {
		if (config.isUpdate()) {
			if (updateDB.isFailed()) {
				mainOutMessage(
						"WW.finish> Update finished with problems, passing exit code 1",
						1);
				System.exit(1);
			}
			mainOutMessage("WW.finish> Database updates were successful", 1);
		} else {
			FileWriter fileWriter = new FileWriter(config.getLOG_NAME(), false);
			PrintWriter out = new PrintWriter(fileWriter, true);
			if (wr.isAnyFails()) {
				out.print("error "
						+ config.getYMDFormatter().format(new Date()));
				mainOutMessage(
						"WW.finish>" + System.lineSeparator() + "   error "
								+ config.getYMDFormatter().format(new Date()),
						1);
				System.exit(1);
			} else {
				out.print("ok " + config.getYMDFormatter().format(new Date()));
				mainOutMessage(
						"WW.finish> Logfile output:" + System.lineSeparator()
								+ "   ok "
								+ config.getYMDFormatter().format(new Date()),
						1);
			}

			out.close();
			fileWriter.close();
		}
		mainOutMessage("WW.finish> Passing exit code 0 (successful run)", 1);
		System.exit(0);
	}

	@Override
	public void stopProgram() {
		config.setStopProgram(true);
	}

	public boolean isStop() {
		return config.isStopProgram();
	}

}
