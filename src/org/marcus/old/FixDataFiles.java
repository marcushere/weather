package org.marcus.old;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;

public class FixDataFiles {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		appendDataFiles();
	}

	public static void appendDataFiles() {
		try {
			String[] zips = WeatherRecorder.scanZips("zips.txt");
			for (int i = 0; i < zips.length; i++) {
				BufferedReader br = new BufferedReader(new FileReader(new File(
						"data\\" + zips[i] + ".1dat")));
				FileWriter fw = new FileWriter("data\\" + zips[i] + ".dat",
						false);
				PrintWriter pw = new PrintWriter(fw, true);
				String line = br.readLine();
				while (line != null) {
					pw.println(line);
					line = br.readLine();
				}
				pw.close();
				fw.close();
				br.close();
			}
		} catch (Exception e) {

		}
	}

	@SuppressWarnings("unused")
	private static void removeLastDay() {
		try {
			String[] zips = WeatherRecorder.scanZips("zips.txt");
			for (int i = 0; i < zips.length; i++) {
				System.out.println(zips[i]);
				File datFile = new File("data\\" + zips[i] + ".dat");
				File tempFile = new File("data\\" + zips[i] + ".temp");
				BufferedReader br = new BufferedReader(new FileReader(datFile));
				BufferedWriter bw = new BufferedWriter(new FileWriter(tempFile));

				String line = br.readLine();

				while (line != null) {
					if (line.charAt(0) == '#') {
						String[] split = line.split(" ");
						String[] date = split[1].split("T");
						if (date[0].equals("2011-08-26")) {
							bw.write(line);
							break;
						}
					}
					bw.write(line);
					bw.newLine();
					line = br.readLine();
				}
				br.close();
				bw.close();

				br = new BufferedReader(new FileReader(tempFile));
				FileWriter fw = new FileWriter(datFile, false);
				PrintWriter out = new PrintWriter(fw, true);

				line = br.readLine();
				while (line != null) {
					out.println(line);
					line = br.readLine();
				}
				br.close();
				fw.close();
				out.close();
				tempFile.delete();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
