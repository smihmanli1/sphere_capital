import sys
import datetime
import subprocess

normalizedDataPath = sys.argv[1]
year = int(sys.argv[2])

currentDate = datetime.datetime(year,1,1)
endDate = datetime.datetime(year+1,1,1)

numParallelJobs = 5
currentlyRunningSubprocesses = []
while currentDate < endDate:
    for i in range(numParallelJobs):
        currentDateString = currentDate.strftime("%Y-%m-%d")
        commandAsList = ['python3', 'bist50_pairs_trading_backtester2.py', normalizedDataPath, currentDateString]
        print (f"Running: {' '.join(commandAsList)}")
        p = subprocess.Popen(commandAsList)
        currentlyRunningSubprocesses.append(p)

        currentDate += datetime.timedelta(days=1)

    for p in currentlyRunningSubprocesses:
        p.communicate()



