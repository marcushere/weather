package org.marcus.old;


public class WeatherTester {

	public static void main(String[] args) {
		try {
			AreaData ad55123 = new AreaData("data/55123.dat","55123");
			ad55123.printDailyData("55123p.dat");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
