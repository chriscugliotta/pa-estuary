REM  First, I downloaded Python 3.5.2 from:
REM  https://www.python.org/downloads/
REM  
REM  Then, I installed Python into:
REM  C:\Python\Python3.5.2\python.exe
REM
REM  Then, I added this path to my PATH environment variable:
REM  C:\Python\Python3.5.2

REM  Logging paths
set directory=%~dp0
set file=%~n0
set log="%directory%%file%.log"
echo Begin > %log%

REM  Python paths
set python="C:\Python\Python3.5.2\python.exe"
set pip="C:\Python\Python3.5.2\Scripts\pip.exe"
set virtualenv="C:\Python\Python3.5.2\Scripts\virtualenv.exe"
set venv="C:\Python\Env\pa-estuary"
set venvActivate="C:\Python\Env\pa-estuary\Scripts\activate.bat"

REM  Install virtualenv
echo Installing virtualenv... >> %log%
%pip% install virtualenv >> %log%

REM  Create new virtualenv instance
echo Creating new virtualenv instance... >> %log%
%virtualenv% %venv% >> %log%

REM  Activate virtualenv instance
echo Activating new virtualenv instance... >> %log%
call %venvActivate% >> %log%

REM  Install Python dependencies
echo Installing Python dependencies... >> %log%
pip install -r requirements.txt >> %log%

REM  Verify
python install.py >> %log% 2>&1

REM  Exit
echo End >> %log%