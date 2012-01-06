package org.marcus.old;

import java.io.BufferedReader;
import java.io.FileReader;

public class AreaData {

	DailyRecordedInfo[] areaData;
	DailyRecord[] dailyRecord;
	String zip;

	public AreaData(String filename, String zipCode) throws Exception {
		zip = zipCode;
		BufferedReader br = new BufferedReader(new FileReader(filename));
		String line = br.readLine();
		int n = 0;
		while (line != null) {
			if (line.charAt(0) == '#') {
				n++;
			}
			line = br.readLine();
		}
		br.close();
		br = new BufferedReader(new FileReader(filename));
		System.out.println();

		areaData = new DailyRecordedInfo[n];
		n = 0;
		line = br.readLine();
		String[] dailyData = new String[7];
		int i = 0;
		while (line != null) {
			dailyData[i] = line;
			i++;
			if (line.charAt(0) == '#') {
				areaData[n] = new DailyRecordedInfo(dailyData, zip);
				n++;
				i = 0;
			}
			line = br.readLine();
		}
		br.close();

		dailyRecord = new DailyRecord[areaData.length - 4];

		for (i = 3; i < areaData.length - 1; i++) {
			dailyRecord[i - 3] = new DailyRecord(areaData[i].date,
					areaData[i - 1].DPT, areaData[i - 1].HPT,
					areaData[i - 3].DPT_3_Day, areaData[i - 3].HPT_3_Day,
					areaData[i + 1].yesterdayDAT, areaData[i + 1].yesterdayHAT);
		}
	}

	public void printAreaData(String filename) throws Exception {
		for (int i = 0; i < areaData.length; i++) {
			areaData[i].printData(filename, true);
		}
	}

	public void printDailyData(String filename) throws Exception {
		for (int i = 0; i < dailyRecord.length; i++) {
			dailyRecord[i].printDailyRecord(filename, true);
		}
	}

}
