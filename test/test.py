import sys
from pathlib import Path
sys.path.append(str(Path('.').absolute().parent))

from ohlc_online import *
from ohlc_offline import *

# incase of failure, change date to latest.

try:
    ET("HDFCBANK", "2024-03-22", "2024-03-26").df(3).to_markdown()
    print("ET - OK")
except:
    print("ET - Error.")

try:
    MC("HDFCBANK", "2024-03-22", "2024-03-26").df(3).to_markdown()
    print("MC - OK")
except:
    print("MC - Error")

try:
    Upstox("HDFCBANK", "2024-03-22", "2024-03-26").df(3).to_markdown()
    print("Upstox - OK")
except:
    print("Upstox - Error")

try:
    BnfOfflineDataSource().getCompleteData().to_markdown()
    print("Offline - OK")
except:
    print("Offline - Error")

try:
    print(HistoricalData("HDFCBANK", "2024-03-22", "2024-03-26").dfForDate("2024-03-22", 3))
    print("GenericClass - OK")
except Exception as e:
    print(f"GenericClass - Error - {e}")

