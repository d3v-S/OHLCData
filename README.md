# nse-ohlc-data

## what is this?
- a bunch of scripts that allows one to download nse-ohlc data from various websites, some realtime and others historically as a pandas dataframe.
- datasources include Moneycontrol, EconomicTimes and Upstox

## how to use?
- as a module:
  * download all the scripts in one folder and create a ```__init__.py```
- just as a script:
  * download the script that is need by you and see the sample test in test folder about calling the apis.
- run test:
  * ```python test.py``` in ```test``` folder, once the whole folder has been downloaded.


## other-info

### files:
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

- Intraday * .zip:
  * contains data from 2015-2023 for BNF and NF in 1-min format

