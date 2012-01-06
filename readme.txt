WeatherRecorder.jar runs with no arguments. It looks for a file in the same
directory called "zips.txt" to get the zip codes. This file is expected to
have one zip code per line with no blank lines.
Each zip code will have a file dedicated to it in the directory "data"
called "ZIPCODE.txt" which will have all of the data in text form. There
are four kinds of records for each day of data. Each of the data rows have
the type followed by the date the data was recorded followed by the date
it's talking about. The program collects data for the next day and the day
three days out. Each data field is separated by "; ".
HourlyPred is the hourly forecast of temperature and precipitation. After the
forecast date comes the data. It consists of the hour followed by the
temperature followed by the chance of precipitation.
DailyPred is the daily forecast. It has the three initial fields followed by
temperature and precipitation chance.
HourlyActual and DailyActual follow the same format except that for
precipitation HA gives the conditions at the hour (eg. cloudy or sunny or
rainy) and DA gives the amount of precipitation over the day.
Every time the program runs, it should write a one-line file called "log.txt" in
the home directory. This will either say "ok" and then the date, or "error" and
then the date. If the file says "error" the program will also print the stack
trace in a file called "error.txt" in the same directory. Each time the program
is launched, it checks to see if it ran successfully that day. If it did not, it
will run. If it did, it will just quit immediately.


Correct precip amounts 12/29/11 and after.