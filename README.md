# OHLCData

## files:

- ohlc_download:
  * downloads data from internet using different sources - ET, MC, Upstox
     - ET: 1 min till 300000 countback
     - MC: 1 min data for one year
     - Upstox: 1 min data for 6 months
       * NSE.csv and BSE.csv (instrument_data/) are required by Upstox Method
  * If we split data into per day, it starts from 9:15, the data before this time has to be ignored.

- ohlc_existing:
  * ```data``` folder has 1 min data for BNF and NF from 2015-2023. 2021-23 are extracted, rest zipped.
  * this is used to analyze data and show in the same dataframe format as the downloaded one.
  * It seems **data is wrong.**
 
- test:
  * shows how to use the methods.


## usage:
### as a module:
- add __init__.py and use it as anymodule.

## test:
- go to test folder and ```python test.py```
