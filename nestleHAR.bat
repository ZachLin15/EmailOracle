sqlplus /nolog @harcust.txt
set source=c:\NESTLE
set destination=P:\
xcopy /s /i /y "%source%" "%destination%"
