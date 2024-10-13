@echo off
echo Starting the batch file...

:: Get current time
for /f "tokens=1-2 delims=: " %%a in ('time /t') do set HOUR=%%a

:: Check if current time is greater than 07:00 PM (19:00)
if %HOUR% LSS 19 (
    echo The current time is less than 07:00 PM. The script can only run after 07:00 PM.
    pause
    exit /b
)

:: Get the current date in YYYY-MM-DD format using wmic
for /f %%x in ('wmic os get localdatetime ^| find "."') do set dt=%%x
set YEAR=%dt:~0,4%
set MONTH=%dt:~4,2%
set DAY=%dt:~6,2%
set DYNAMIC_DATE=%YEAR%-%MONTH%-%DAY%

echo Current date: %DYNAMIC_DATE%

:: Set the default number of days (1)
set ARG_DAYS=%2
if "%ARG_DAYS%"=="" set ARG_DAYS=1

echo Number of days: %ARG_DAYS%

:: Navigate to the folder containing your virtual environment
cd /d D:\New_Setup_Code\NSE
if errorlevel 1 (
    echo Failed to navigate to the directory. Check the path.
    pause
    exit /b
)

:: Activate the virtual environment
echo Activating virtual environment...
call nse_env\Scripts\activate
if errorlevel 1 (
    echo Failed to activate the virtual environment. Check the venv path.
    pause
    exit /b
)

:: Run the Python script with the current date and number of days as arguments
echo Running Python script...
python main.py %DYNAMIC_DATE% %ARG_DAYS%
if errorlevel 1 (
    echo Python script execution failed.
    pause
    exit /b
)

:: Deactivate the virtual environment after execution
deactivate
if errorlevel 1 (
    echo Failed to deactivate the virtual environment.
    pause
    exit /b
)

:: Hold the window open
echo Script executed successfully.
pause
