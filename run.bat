@if exist errorlevel del errorlevel
"C:\program files\java\jre7\bin\java" -Djava.library.path="C:\Tools\Microsoft SQL Server JDBC Driver 3.0\sqljdbc_3.0\enu\auth\x86" -jar WeatherRecorder.jar -d4 -t8 --term
@if errorlevel 1 echo %errorlevel%
@if errorlevel 1 pause
@if not errorlevel 1 "C:\program files\java\jre7\bin\java" -Djava.library.path="C:\Tools\Microsoft SQL Server JDBC Driver 3.0\sqljdbc_3.0\enu\auth\x86" -jar WeatherRecorder.jar -d4 -t8 --term --update
@if not errorlevel 1 echo finished
@if not errorlevel 1 PING 1.1.1.1 -n 1 -w 20000 >NUL
@echo %errorlevel% >>errorlevel