package org.marcus.weather;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DataFetcher {

	private static class LineReader {
		private BufferedReader reader;
		private String line;
		private PrintWriter out;

		LineReader(Reader streamReader, PrintWriter out) throws IOException {
			this.reader = new BufferedReader(streamReader);
			this.out = out;
			readLine();
		}

		private void readLine() throws IOException {
			this.line = reader.readLine();
			if (out != null) {
				out.println(line);
			}
		}

		public String getStuff() {
			int end = line.indexOf("</");
			if (end < 0) {
				return "";
			}
			String s = line.substring(0, end);
			int pos = s.lastIndexOf(">");
			return s.substring(pos + 1);
		}

		public void skipTo(String find) throws IOException {
			while (!line.contains(find)) {
				readLine();
			}
		}

		public Float getFloatStuff() {
			String stuff = getStuff();
			try {
				return Float.valueOf(stuff);
			} catch (NumberFormatException e) {

			}
			return null;
		}

	}

	int dayNumber1;
	int dayNumber3;
	public String zip;
	private String date;
	public ForecastData forecast1;
	public ForecastData forecast3;
	public PastData past;
	public boolean valid = false;

	// temporary variables for getting the data
	private HourlyPast[] hp = new HourlyPast[24];
	private OverallPast op;
	private HourlyForecast[] hf1;
	private OverallForecast of1;
	private HourlyForecast[] hf3;
	private OverallForecast of3;

	public DataFetcher(String zipCode) throws Exception {
		zip = zipCode;
		this.date = new SimpleDateFormat("yyyy-MM-dd").format(Calendar
				.getInstance().getTime());
		Calendar cal = Calendar.getInstance();
		dayNumber1 = cal.get(Calendar.DAY_OF_YEAR);
		dayNumber3 = dayNumber1 + 2;
		wundergroundFuture();
		wundergroundPast();
		valid = true;
	}

	private void wundergroundFuture() throws IOException, InterruptedException {
		getOverallForecast();
		getHourlyForecast(this.dayNumber1, false);
		getHourlyForecast(this.dayNumber3, true);

		this.forecast1 = new ForecastData(this.zip, this.date, this.of1,
				this.hf1);
		this.forecast3 = new ForecastData(this.zip, this.date, this.of3,
				this.hf3);
	}

	private void getHourlyForecast(int dayNumber, boolean threeDay) {

	}

	private void getOverallForecast() throws IOException {
		for (int tries = 0; tries < 7; tries++) {
			File outFile;
			FileWriter fileWriter;
			SimpleDateFormat format = new SimpleDateFormat(
					"yyyyMMdd'T'kkmmssSSS");
			outFile = new File("test/test"
					+ format.format(Calendar.getInstance().getTime())
					+ "overall.html");
			fileWriter = new FileWriter(outFile, false);
			PrintWriter out = new PrintWriter(fileWriter, true);
			out.println(MakeURL.overallURL(this.zip));
			InputStreamReader webStream = null;
			try {
				URL url = new URL(MakeURL.overallURL(this.zip));
				webStream = new InputStreamReader(url.openStream());
				LineReader lineReader = new LineReader(webStream, out);

				lineReader.skipTo(this.zip);
				lineReader.readLine();
				while (lineReader.line.contains("</div>")) {
					lineReader.skipTo("fct_day_" + dayNumber1);
					
				}

				out.close();
				fileWriter.close();
				outFile.delete();
				webStream.close();
				return;
			} catch (NullPointerException e) {

			}
		}
	}

	private void wundergroundPast() throws Exception {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(cal.getTimeInMillis() - 1000 * 3600 * 24);
		wundergroundPast(cal.getTime());
	}

	private void wundergroundPast(Date date) throws Exception {
		for (int tries = 0; tries < 7; tries++) {
			File outfile;
			FileWriter fileWriter;
			SimpleDateFormat format = new SimpleDateFormat(
					"yyyyMMdd'T'kkmmssSSS");
			outfile = new File("test\\test"
					+ format.format(Calendar.getInstance().getTime()) + ".html");
			fileWriter = new FileWriter(outfile, false);
			PrintWriter out = new PrintWriter(fileWriter, true);
			out.println(MakeURL.pastWundergroundURL(zip, date));
			InputStreamReader webStream = null;
			try {

				URL url = new URL(MakeURL.pastWundergroundURL(zip, date));

				webStream = new InputStreamReader(url.openStream());
				getPastWData(new LineReader(webStream, out));
				this.past = new PastData(this.zip, this.date, this.op, this.hp);

				// System.out.println(op.high + ", " + op.precip);
				// for (int i = 0; i < 24; i++) {
				// System.out.println(hp[i].hour + ", " + hp[i].temp
				// + ", " + hp[i].precip + ", " + hp[i].conditions);
				// }

				webStream.close();
				out.close();
				fileWriter.close();
				outfile.delete();
				return;

			} catch (NullPointerException e) {
				if (webStream != null)
					webStream.close();
				System.out.println("Failed to read");
				out.close();
				fileWriter.close();
				Thread.sleep(1000);
				if (this.op == null) {
					this.op = new OverallPast();
				}
			}
		}
	}

	private void getPastWData(LineReader reader) throws IOException, Exception {

		for (int i = 0; i < 24; i++) {
			this.hp[i] = new HourlyPast(i);
		}
		this.op = new OverallPast();

		Float snow = null;

		reader.skipTo("contentData");

		reader.skipTo("<tbody>");
		reader.readLine();
		while (reader.line.contains("<tr")) {
			reader.readLine();
			if (reader.line.contains("indent")) {
				String stuff = reader.getStuff();
				if (stuff.equalsIgnoreCase("Max Temperature")) {
					reader.readLine();
					reader.readLine();
					this.op.high = reader.getFloatStuff();
				} else if (stuff.equalsIgnoreCase("Precipitation")) {
					reader.readLine();
					reader.readLine();
					this.op.precip = reader.getFloatStuff();
				} else if (stuff.equalsIgnoreCase("Snow")) {
					reader.readLine();
					reader.readLine();
					snow = reader.getFloatStuff();
				}
			}

			reader.skipTo("/tr");
			reader.readLine();
		}

		if ((this.op.precip != null && snow != null)) {
			this.op.precip = this.op.precip + snow;
		} else if (this.op.precip != null) {

		} else if (snow != null) {
			this.op.precip = snow;
		}

		reader.skipTo("observations_details");

		int colNum = 0;
		int tempCol = -1;
		int precipCol = -1;
		int condCol = -1;
		while (!reader.line.contains("/thead")) {
			if (reader.line.contains("<th>")) {
				colNum++;
				if (reader.line.contains("Temp.")) {
					tempCol = colNum;
				} else if (reader.line.contains("Precip")) {
					precipCol = colNum;
				} else if (reader.line.contains("Conditions")) {
					condCol = colNum;
				}
			}
			reader.readLine();
		}

		reader.skipTo("<tbody>");
		reader.readLine();
		int i = 0;
		while (reader.line.contains("<tr")) {
			colNum = 0;
			while (!reader.line.contains("/tr")) {
				if (reader.line.contains("<td")) {
					colNum++;
					if (colNum == 1) {
						String stuff = reader.getStuff();
						int colonidx = stuff.indexOf(":");
						String hourStr = stuff.substring(0, colonidx);
						int hour = Integer.parseInt(hourStr);
						if (hour == 12) {
							hour = 0;
						}
						if (stuff.endsWith("PM")) {
							hour = hour + 12;
						}
						i = hour;
						this.hp[i].hour = hour;
					} else if (!hp[i].conditions.equals("")) {
					} else if (tempCol == colNum) {
						reader.readLine();
						this.hp[i].temp = reader.getFloatStuff();
					} else if (precipCol == colNum) {
						if (reader.line.contains("N/A")) {
							this.hp[i].precip = null;
						} else {
							reader.readLine();
							this.hp[i].precip = reader.getFloatStuff();
						}
					} else if (condCol == colNum) {
						this.hp[i].conditions = reader.getStuff();
					}
				}
				reader.readLine();
			}
			reader.readLine();
		}
	}
}
