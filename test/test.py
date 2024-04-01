import sys
from pathlib import Path
sys.path.append(str(Path('.').absolute().parent))

from ohlc_online import *
from ohlc_offline import *

# incase of failure, change date to latest.

try:
    ET("HDFCBANK", "2024-03-22", "2024-03-26").df(3).to_markdown()
except:
    print("ET error.")

try:
    MC("HDFCBANK", "2024-03-22", "2024-03-26").df(3).to_markdown()
except:
    print("MC error")

try:
    Upstox("HDFCBANK", "2024-03-22", "2024-03-26").df(3).to_markdown()
except:
    print("Upstox error")

try:
    BnfOfflineDataSource().getCompleteData().to_markdown()
except:
    print("Offline Error")
