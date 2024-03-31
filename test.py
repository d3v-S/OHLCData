from ohlc_online import *
from ohlc_offline import *

# print(ET("HDFCBANK", "2024-03-22", "2024-03-26").df(3).to_markdown())
# print(MC("HDFCBANK", "2024-03-22", "2024-03-26").df(3).to_markdown())
# print(Upstox("HDFCBANK", "2024-03-22", "2024-03-26").df(3).to_markdown())

print(BnfOfflineDataSource().getCompleteData().to_markdown())
