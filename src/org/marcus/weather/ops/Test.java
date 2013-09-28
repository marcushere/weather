package org.marcus.weather.ops;

import org.marcus.weather.MakeURL;

public class Test {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		System.out.println(MakeURL.overallURL("55123"));
		System.out.println(MakeURL.hourlyURL("55123", 3));
	}
}
