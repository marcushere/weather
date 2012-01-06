package org.marcus.weather;

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
