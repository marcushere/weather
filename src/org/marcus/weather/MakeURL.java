package org.marcus.weather;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Calendar;
import java.util.Date;

public class MakeURL {

	public static String hourlyURL(String zip, Integer futureDays)
			throws IOException {
		Calendar cal = Calendar.getInstance();
		int day = 0;
		if (cal.get(Calendar.DAY_OF_YEAR) - 1 + futureDays > 364) {
			day = cal.get(Calendar.DAY_OF_YEAR) - 366 + futureDays;
		} else {
			day = cal.get(Calendar.DAY_OF_YEAR) - 1 + futureDays;
		}
		return overallURL(zip) + "&hourly=1" + "&yday=" + day;
	}

	public static String overallURL(String zip) throws IOException {
		String pastWundergroundURL = pastWundergroundURL(zip);
		System.out.println(pastWundergroundURL);
		URL url1 = new URL(pastWundergroundURL);
		BufferedReader br = new BufferedReader(new InputStreamReader(
				url1.openStream()));
		String line = br.readLine();
		try {
			while (!line.contains("View Current Conditions")) {
				line = br.readLine();
			}
			return "http://www.wunderground.com"
					+ line.substring((line.indexOf("href=\"") + 6),
							line.indexOf("\">View"));
		} catch (NullPointerException e) {
			return "http://www.wunderground.com/cgi-bin/findweather/getForecast?query="
					+ zip;
		}
	}

	public static String pastOverallURL(String zip) {
		return "http://www.weather.com/weather/pastweather/" + zip;
	}

	public static String pastHourlyURL(String zip) {
		return "http://www.weather.com/weather/pastweather/hourly/" + zip;
	}

	public static String pastWundergroundURL(String zip, Date date)
			throws IOException {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		String url1 = "http://www.wunderground.com/cgi-bin/findweather/getForecast?airportorwmo=query&historytype=DailyHistory&backurl=%2Fhistory%2Findex.html&code="
				+ zip
				+ "&month="
				+ (cal.get(Calendar.MONTH) + 1)
				+ "&day="
				+ cal.get(Calendar.DATE) + "&year=" + cal.get(Calendar.YEAR);
		URL url = new URL(url1);
		URLConnection con = url.openConnection();
		con = makeCon(con);
		URL url2 = new URL("http://www.wunderground.com"
				+ con.getHeaderField("location"));
		BufferedReader br = new BufferedReader(new InputStreamReader(
				url2.openStream()));
		try {
			while (!br.readLine().contains("not official NWS values"))
				;
		} catch (NullPointerException e) {
			return "http://www.wunderground.com"
					+ con.getHeaderField("location");
		}
		String line = br.readLine();
		int index = line.indexOf("href=\"") + 6;
		return "http://www.wunderground.com"
				+ line.substring(index, line.lastIndexOf("\">"));
	}

	private static URLConnection makeCon(URLConnection con) {
		con.setDoOutput(true);
		((HttpURLConnection) con).setInstanceFollowRedirects(false);
		con.setRequestProperty("Accept",
				"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
		con.setRequestProperty("Accept-Charset",
				"ISO-8859-1,utf-8;q=0.7,*;q=0.3");
		con.setRequestProperty("Accept-Encoding", "gzip,deflate,sdch");
		con.setRequestProperty("Accept-Language", "en-US,en;q=0.8,de;q=0.6");
		con.setRequestProperty("Connection", "keep-alive");
		con.setRequestProperty("Cache-Control", "no-cache");
		con.setRequestProperty("Host", "www.wunderground.com");
		con.setRequestProperty("Referer",
				"http://www.wunderground.com/history/");
		con.setRequestProperty(
				"User-Agent",
				"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/535.7 (KHTML, like Gecko) Chrome/16.0.912.63 Safari/535.7");
		return con;
	}

	public static String pastWundergroundURL(String zip) throws IOException {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(cal.getTimeInMillis() - 1000 * 3600 * 24);
		return pastWundergroundURL(zip, cal.getTime());
	}

}
