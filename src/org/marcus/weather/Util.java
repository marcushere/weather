package org.marcus.weather;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Util {

	public static void exceptionHandler(Exception e, int exitCode,
			String outputMessage, Config config, WeatherUI wui) throws IOException {
		if (exitCode > -1) {
			FileWriter fileWriter;
			fileWriter = new FileWriter(config.getLOG_NAME(), false);
			PrintWriter out = new PrintWriter(fileWriter, true);
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			out.print("error " + format.format(new Date()));

			out.close();
			printError(e, "Output message: " + outputMessage);
			wui.mainOutMessage(outputMessage, 1);
			System.exit(exitCode);
		} else {
			printError(e, "Output message: " + outputMessage);
			wui.mainOutMessage(outputMessage, -exitCode);
		}
	}
	
	private static void printError(Exception e, String zip) throws IOException {
		FileWriter fileWriter;
		PrintWriter out;
		fileWriter = new FileWriter(Config.ERROR_NAME, true);
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
	
}
