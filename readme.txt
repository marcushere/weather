weatherrecorder.jar runs with the following argument:

java -Djava.library.path="[path to sql server driver]" -jar WeatherRecorder.jar

It scans the zip codes in the home directory file called "zips.txt" with one zip code on each line. For each zip code it gets the past and forecast data for hourly and daily information and writes all of it to a SQLServer database.

When the program runs, it will write in the file called "log.txt" in the home directory. If it completed successfuly, it will output in the format "ok [date]" where [date] is the current date in the format yyyy-mm-dd. If it exits early, it will write "error [date] [zip]" where the date is as described earlier and the [zip] is the zip code that it failed on. When run again, the program will check the logfile and start with the zip code that failed the last time. This is to avoid redoing as much as possible. When there is an error, the stack trace along with the exact time and date and the zip code will be appened to the file "error.txt" along with java's comment on the error.

The program will exit with an errorlevel dependent on the location at which the error occured. Almost all exceptions are dealt with in the main class, so all of the "System.exit(n)" statements are in the file WeatherRecorder.java.

In order to force the program to exit early, the user can type "c" at any time and the program will exit with an error on the next zip code.

Command line arguments are:
	-csv	to write to a csv file
	-d	to debug (writes out a lot more information)
	help	to display this information

Normally the program will only write out each zip code as it starts collecting the data for it. If the file run.bat is used to run the program, it will print "finished" when the program exits with status 0.