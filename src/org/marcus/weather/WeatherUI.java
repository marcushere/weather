package org.marcus.weather;

import java.io.IOException;

public interface WeatherUI {

	public void mainOutMessage(String str, int errorlevel);

	public void threadOutMessage(String str, int threadID,
			int errorlevel);

	public void stopProgram();
	
	public void finish() throws IOException;

}
