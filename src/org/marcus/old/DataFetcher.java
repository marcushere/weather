package org.marcus.old;

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

	public int dayNumber;
	public String zip;
	public int plusDays;
	public int wundergroundHours;
	public String[] wundergroundTimes;

	int layers = 0;
	// private boolean valid;
	private int[] pastTimes = new int[24];
	private Float[] tempsPast = new Float[24];
	private Float[] precipPast = new Float[24];
	private String[] conditions = new String[24];
	private Float[] pastWeather = new Float[2];

	public Date pastDate;

	/*
	 * getForecastTime must be run immediately after the constructor in order
	 * for the hourly forecasts to work correctly.
	 */

	public DataFetcher(String zipCode, int futureDays) throws Exception {
		zip = zipCode;
		plusDays = futureDays;
		Calendar cal = Calendar.getInstance();
		dayNumber = cal.get(Calendar.DAY_OF_YEAR) - 1 + futureDays;
		if (futureDays == 1)
			wundergroundPast();
	}

	public DataFetcher(String zipCode, Date date) throws Exception {
		zip = zipCode;
		wundergroundPast(date);
		pastDate = date;
	}

	private int[] getPastTimes() {
		for (int i = 0; i < 24; i++) {
			pastTimes[i] = i;
		}
		return pastTimes;
	}

	public String[] getForecastTimes() throws Exception {
		String[] times = new String[8];
		try {
			layers++;
			BufferedReader br = new BufferedReader(new InputStreamReader(
					new URL(MakeURL.hourlyURL(zip, plusDays)).openStream()));

			// FileWriter fileWriter = new FileWriter("test.html", false);
			// PrintWriter out = new PrintWriter(fileWriter, true);
			// out.println(MakeURL.hourlyURL(zip, plusDays) + " times");

			int numHours = 0;

			String line = "";
			line = br.readLine();
			// out.println(line);
			int index = 0;
			int i = 0;

			boolean prevLineWasHour = false;

			// FileWriter fileWriter;
			// PrintWriter out;
			// fileWriter = new FileWriter("whours.html", false);
			// out = new PrintWriter(fileWriter, true);

			while (true) {
				times[i] = "";
				// out.println(line);
				if (line.contains("<th class=\"taC\">")) {
					prevLineWasHour = true;
					index = line.indexOf("<th class=\"taC\">") + 16;
					while (Character.isDigit(line.charAt(index))) {
						times[i] = times[i] + line.charAt(index);
						index++;
					}
					i++;
					if (i > 7) {
						break;
					}
				} else if (prevLineWasHour) {
					numHours = i;
					// valid = true;
					break;
				} else if (line.contains("<h1>There has been an error!</h1>")) {
					numHours = 0;
					// valid = false;
					break;
				}
				line = br.readLine();
				// out.println(line);
			}

			numHours = i;

			wundergroundHours = numHours;
			String[] ret = new String[numHours];
			for (int j = 0; j < numHours; j++) {
				ret[j] = times[j];
			}
			wundergroundTimes = ret;

			// out.close();
			// fileWriter.close();

			layers = 0;
			return ret;
		} catch (Exception e) {
			if (layers > 5) {
				// valid = false;
				return times;
			} else {
				Thread.sleep(2000);
				return getHourlyForecastTemps();
			}
		}

	}

	public String[] getHourlyForecastTemps() throws Exception {
		BufferedReader br = new BufferedReader(new InputStreamReader(new URL(
				MakeURL.hourlyURL(zip, plusDays)).openStream()));

		// FileWriter fileWriter = new FileWriter("test.html", false);
		// PrintWriter out = new PrintWriter(fileWriter, true);
		// out.println(MakeURL.hourlyURL(zip, plusDays) + " temps");

		String[] temps = new String[wundergroundHours];

		// if (valid) {
		try {
			layers++;
			String line = "";
			line = br.readLine();
			// if (plusDays == 1)
			// out.println(line);
			int i = 0;
			int index = 0;

			while (i < wundergroundHours) {
				temps[i] = "";
				if (line.contains("<td class=\"taC\"")) {
					line = br.readLine();
					// if (plusDays == 1)
					// out.println(line);
					if (line.contains("<div>")) {
						index = line.indexOf("<div>") + 5;
						while (Character.isDigit(line.charAt(index))
								| line.charAt(index) == '-') {
							temps[i] = temps[i] + line.charAt(index);
							index++;
						}
						i++;
					} else {
						System.out.println(plusDays + " " + zip + " " + line);
						throw new Exception();
					}
				}
				line = br.readLine();
				// if (plusDays==1) out.println(line);
			}

			if (i < wundergroundHours) {
				throw new Exception();
			}
			layers = 0;
			// out.close();
			return temps;
		} catch (Exception e) {
			// out.close();
			if (layers > 5) {
				for (int i = 0; i < wundergroundHours; i++) {
					temps[i] = "";
				}
				WeatherRecorder.printError(e, zip);
				layers = 0;
				return temps;
			} else {
				Thread.sleep(2000);
				return getHourlyForecastTemps();
			}
		}
		// } else {
		// return new String[0];
		// }
	}

	public String[] getHourlyForecastPrecip() throws Exception {
		String[] precips = new String[wundergroundHours];
		FileWriter fileWriter = new FileWriter("test.html", false);
		PrintWriter out = new PrintWriter(fileWriter, true);
		out.println(MakeURL.hourlyURL(zip, plusDays) + " precips");

		try {
			layers++;
			BufferedReader br = new BufferedReader(new InputStreamReader(
					new URL(MakeURL.hourlyURL(zip, plusDays)).openStream()));

			// if (valid) {
			String line = "";
			line = br.readLine();
			if (plusDays == 1)
				out.println(line);
			int i = 0;

			while (line != null
					&& !line.contains("Probability of Precipitation (%)")) {
				line = br.readLine();
				if (plusDays == 1)
					out.println(line);
			}

			i = 0;

			while (i < wundergroundHours) {
				precips[i] = "";
				if (line.contains("<td class=\"taC\"")) {
					line = br.readLine();
					if (plusDays == 1)
						out.println(line);
					int index = 0;
					while (!(Character.isDigit(line.charAt(index)) | line
							.charAt(index) == '-')) {
						index++;
					}

					while (Character.isDigit(line.charAt(index))
							| line.charAt(index) == '-') {
						precips[i] = precips[i] + line.charAt(index);
						index++;
					}
					i++;
				}
				line = br.readLine();
				if (plusDays == 1)
					out.println(line);
			}

			if (i < wundergroundHours) {
				throw new Exception();
			}
			layers = 0;
			out.close();
			return precips;
			// } else {
			// return new String[0];
			// }
		} catch (Exception e) {
			out.close();
			if (layers > 5) {
				for (int i = 0; i < wundergroundHours; i++) {
					precips[i] = "";
				}
				WeatherRecorder.printError(e, zip);
				layers = 0;
				return precips;
			} else {
				Thread.sleep(2000);
				return getHourlyForecastTemps();
			}
		}
	}

	public String[] getOverallForecast() throws Exception {
		String[] forecast = new String[2];
		try {
			layers++;
			BufferedReader br = new BufferedReader(new InputStreamReader(
					new URL(MakeURL.overallURL(zip)).openStream()));

			// FileWriter fileWriter = new FileWriter("test.html", false);
			// PrintWriter out = new PrintWriter(fileWriter, true);
			// out.println(MakeURL.overallURL(zip));

			// if (valid) {
			String line = "";
			line = br.readLine();
			// out.println(line);

			int index = 0;

			forecast[0] = forecast[1] = "";

			while (line != null
					&& !line.contains("<div id=\"fct_day_" + dayNumber + "\"")) {
				line = br.readLine();
				// out.println(line);
			}

			while (!line.contains("<div class=\"fctHiLow\">")) {
				line = br.readLine();
				// out.println(line);
			}

			line = br.readLine();
			// out.println(line);

			if (line == null) {
				throw new Exception();
			}

			if (line.contains("<span class=\"b\">")) {
				index = line.indexOf("<span class=\"b\">") + 16;
				while (Character.isDigit(line.charAt(index))
						| line.charAt(index) == '-') {
					forecast[0] = forecast[0] + line.charAt(index);
					index++;
				}
			} else {
				throw new Exception();
			}

			while (!line.contains("<div class=\"popValue\">")) {
				line = br.readLine();
				// out.println(line);
			}

			index = line.indexOf("<div class=\"popValue\">") + 22;
			while (Character.isDigit(line.charAt(index))) {
				forecast[1] = forecast[1] + line.charAt(index);
				index++;
			}

			layers = 0;
			return forecast;
			// } else {
			// String[] ret = new String[2];
			// ret[0] = ret[1] = "";
			// return ret;
			// }
		} catch (Exception e) {
			if (layers > 5) {
				for (int i = 0; i < 1; i++) {
					forecast[i] = "";
				}
				WeatherRecorder.printError(e, zip);
				layers = 0;
				return forecast;
			} else {
				Thread.sleep(2000);
				return getHourlyForecastTemps();
			}
		}
	}

	public String[] getOverallPast() throws Exception {
		String[] overallPast = new String[2];
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(
					new URL(MakeURL.pastOverallURL(zip)).openStream()));

			// FileWriter fileWriter = new FileWriter("test.html", false);
			// PrintWriter out = new PrintWriter(fileWriter, true);
			// out.println(MakeURL.pastOverallURL(zip));

			String line = br.readLine();
			// out.println(line);

			while (line == null) {
				line = br.readLine();
				// out.println(line);
			}

			while (!line.contains("Actuals")) {
				line = br.readLine();
				// out.println(line);
			}

			while (!(line.contains("&deg;F"))) {
				line = br.readLine();
				// out.println(line);
			}

			int index = 0;
			overallPast[0] = "";
			while (!(Character.isDigit(line.charAt(index)) | line.charAt(index) == '-')) {
				index++;
			}

			while (Character.isDigit(line.charAt(index))
					| line.charAt(index) == '-') {
				overallPast[0] = overallPast[0] + line.charAt(index);
				index++;
			}

			overallPast[1] = "";
			while (!line.contains("in.")) {
				line = br.readLine();
				// out.println(line);
			}

			index = 0;

			while (!(Character.isDigit(line.charAt(index)) | line.charAt(index) == '.')) {
				index++;
			}

			while (Character.isDigit(line.charAt(index))
					| line.charAt(index) == '.') {
				overallPast[1] = overallPast[1] + line.charAt(index);
				index++;
			}

			return overallPast;
		} catch (Exception e) {
			overallPast[0] = "";
			overallPast[1] = "";
			WeatherRecorder.printError(e, zip);
			return overallPast;
		}
	}

	public String[] getHourlyPastTemp() throws Exception {
		String[] tempsPast = new String[pastTimes.length];
		try {
			layers++;
			BufferedReader br = new BufferedReader(new InputStreamReader(
					new URL(MakeURL.pastHourlyURL(zip)).openStream()));

			// FileWriter fileWriter = new FileWriter("test.html", false);
			// PrintWriter out = new PrintWriter(fileWriter, true);
			// out.println(MakeURL.pastHourlyURL(zip));

			String line = br.readLine();
			// out.println(line);
			// System.out.println(line);

			for (int i = 0; i < pastTimes.length; i++) {
				tempsPast[i] = "";
			}

			for (int i = 0; i < pastTimes.length; i++) {
				int index = 0;
				try {
					if (pastTimes.length - 1 - i >= 2) {
						while (!line.contains("<b>" + pastTimes[i] + ":")
								&& !line.contains("<b>" + pastTimes[i + 1]
										+ ":")
								&& !line.contains("<b>" + pastTimes[i + 2]
										+ ":")) {
							line = br.readLine();
							// out.println(out);
						}
					} else if (pastTimes.length - 1 - i == 1) {
						while (!line.contains("<b>" + pastTimes[i] + ":")
								&& !line.contains("<b>" + pastTimes[i + 1]
										+ ":")) {
							line = br.readLine();
							// out.println(out);
						}
					} else if (pastTimes.length - 1 - i == 0) {
						while (!line.contains("<b>" + pastTimes[i] + ":")) {
							line = br.readLine();
							// out.println(out);
						}
					}

					if (line.contains("<b>" + pastTimes[i + 1] + ":")) {
						i++;
					} else if (line.contains("<b>" + pastTimes[i + 2] + ":")) {
						i += 2;
					}

					while (!line.contains("&deg;F")) {
						line = br.readLine();
						// out.println(line);
					}
					index = line.indexOf("<b>") + 3;
					while (Character.isDigit(line.charAt(index))
							| line.charAt(index) == '-') {
						tempsPast[i] = tempsPast[i] + line.charAt(index);
						index++;
					}
				} catch (Exception e) {
					// out.close();
					// fileWriter.close();
					return tempsPast;
				}
			}
			// out.close();
			// fileWriter.close();
		} catch (Exception e) {
			if (layers > 5) {
				for (int i = 0; i < wundergroundHours; i++) {
					tempsPast[i] = "";
				}
				WeatherRecorder.printError(e, zip);
				layers = 0;
				return tempsPast;
			} else {
				Thread.sleep(2000);
				return getHourlyForecastTemps();
			}
		}
		layers = 0;
		return tempsPast;
	}

	public String[] getHourlyPastPrecip() throws Exception {
		String[] precipPast = new String[pastTimes.length];
		try {
			layers++;
			BufferedReader br = new BufferedReader(new InputStreamReader(
					new URL(MakeURL.pastHourlyURL(zip)).openStream()));

			// FileWriter fileWriter = new FileWriter("test.html", false);
			// PrintWriter out = new PrintWriter(fileWriter, true);
			// out.println(MakeURL.pastHourlyURL(zip));

			String line = br.readLine();
			// out.println(out);

			for (int i = 0; i < pastTimes.length; i++) {
				precipPast[i] = "";
			}
			for (int i = 0; i < pastTimes.length; i++) {
				try {
					if (pastTimes.length - 1 - i >= 2) {
						while (!line.contains("<b>" + pastTimes[i] + ":")
								&& !line.contains("<b>" + pastTimes[i + 1]
										+ ":")
								&& !line.contains("<b>" + pastTimes[i + 2]
										+ ":")) {
							line = br.readLine();
							// out.println(out);
						}
					} else if (pastTimes.length - 1 - i == 1) {
						while (!line.contains("<b>" + pastTimes[i] + ":")
								&& !line.contains("<b>" + pastTimes[i + 1]
										+ ":")) {
							line = br.readLine();
							// out.println(out);
						}
					} else if (pastTimes.length - 1 - i == 0) {
						while (!line.contains("<b>" + pastTimes[i] + ":")) {
							line = br.readLine();
							// out.println(out);
						}
					}

					if (line.contains("<b>" + pastTimes[i + 1] + ":")) {
						i++;
					} else if (line.contains("<b>" + pastTimes[i + 2] + ":")) {
						i += 2;
					}

					while (!line.contains("&deg;F")) {
						line = br.readLine();
						// out.println(out);
					}

					int index = line.indexOf("10\">") + 4;
					while (line.charAt(index) != '<') {
						precipPast[i] = precipPast[i] + line.charAt(index);
						index++;
					}
				} catch (Exception e) {
					return precipPast;
				}
			}
		} catch (Exception e) {
			if (layers > 5) {
				for (int i = 0; i < wundergroundHours; i++) {
					precipPast[i] = "";
				}
				WeatherRecorder.printError(e, zip);
				layers = 0;
				return precipPast;
			} else {
				Thread.sleep(2000);
				return getHourlyForecastTemps();
			}
		}
		layers = 0;
		return precipPast;

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

				// System.out.println(pastWeather[0] + ", " + pastWeather[1]);
				// for (int i = 0; i < 24; i++) {
				// System.out.println(pastTimes[i] + ", " + tempsPast[i]
				// + ", " + precipPast[i] + ", " + conditions[i]);
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
				if (this.pastWeather == null) {
					this.pastWeather = new Float[2];
					pastWeather[0] = null;
					pastWeather[1] = null;
				}
			}
		}
	}

	public Float[] getHourlyPastWTemp() {
		return tempsPast;
	}

	public Float[] getHourlyPastWPrecip() {
		return precipPast;
	}

	public String[] getHourlyPastWCond() {
		return conditions;
	}

	public Float[] getOverallWPast() {
		return pastWeather;
	}

	public int[] getPastWTimes() {
		return pastTimes;
	}

	private void getPastWData(LineReader reader) throws IOException, Exception {

		for (int i = 0; i < 24; i++) {
			tempsPast[i] = null;
			precipPast[i] = null;
			conditions[i] = "";
			if (i < 2) {
				pastWeather[i] = null;
			}
		}
		pastTimes = getPastTimes();

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
					pastWeather[0] = reader.getFloatStuff();
				} else if (stuff.equalsIgnoreCase("Precipitation")) {
					reader.readLine();
					reader.readLine();
					pastWeather[1] = reader.getFloatStuff();
				} else if (stuff.equalsIgnoreCase("Snow")) {
					reader.readLine();
					reader.readLine();
					snow = reader.getFloatStuff();
				}
			}

			reader.skipTo("/tr");
			reader.readLine();
		}

		if ((pastWeather[1] != null && snow != null)) {
			pastWeather[1] = pastWeather[1] + snow;
		} else if (pastWeather[1] != null) {

		} else if (snow != null) {
			pastWeather[1] = snow;
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
						pastTimes[i] = hour;
					} else if (!conditions[i].equals("")) {
					} else if (tempCol == colNum) {
						reader.readLine();
						tempsPast[i] = reader.getFloatStuff();
					} else if (precipCol == colNum) {
						if (reader.line.contains("N/A")) {
							precipPast[i] = null;
						} else {
							reader.readLine();
							precipPast[i] = reader.getFloatStuff();
						}
					} else if (condCol == colNum) {
						conditions[i] = reader.getStuff();
					}
				}
				reader.readLine();
			}
			reader.readLine();
		}
	}
}
