@if exist errorlevel del errorlevel
@java -Djava.library.path="C:\Tools\Microsoft SQL Server JDBC Driver 3.0\sqljdbc_3.0\enu\auth\x86" -jar WeatherRecorder.jar -csv
@echo %errorlevel% >>errorlevel